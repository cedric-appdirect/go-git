package git

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-git/go-git/v6/plumbing/format/gitignore"
	"github.com/go-git/go-git/v6/plumbing/object"
)

// BenchmarkAlternatesPerformance benchmarks git operations with alternates.
// This simulates a real-world scenario where a template repository with many files
// is cloned using alternates, then new commits are made and pushed.
//
// Setup:
//   - Creates a "template" repository with 40,000 files in 10 directories
//   - Files are committed in batches (40 commits of 1,000 files each)
//   - Clones the template using Shared: true (sets up alternates automatically)
//   - Performs commits in the work repository (testing alternates performance)
//
// This benchmark measures the performance improvement from caching ObjectStorage
// instances for alternates, which eliminates repeated initialization overhead.
func BenchmarkAlternatesPerformance(b *testing.B) {
	// Skip in short mode as this takes a while to set up
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	// Create temporary directories
	tempDir, err := os.MkdirTemp("", "git-alternates-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	templatePath := filepath.Join(tempDir, "template")
	workPath := filepath.Join(tempDir, "work")

	// Phase 1: Create template repository with 40,000 files
	b.Log("Setting up template repository with 40,000 files...")
	setupStart := b.Elapsed()

	templateRepo, err := PlainInit(templatePath, false)
	if err != nil {
		b.Fatal(err)
	}

	templateWorkTree, err := templateRepo.Worktree()
	if err != nil {
		b.Fatal(err)
	}

	// Add 40,000 files in 10 directories (4,000 files per directory)
	// Commit every 1,000 files to keep it manageable
	const (
		totalFiles     = 40000
		dirsCount      = 10
		filesPerDir    = totalFiles / dirsCount
		filesPerCommit = 1000
	)

	b.Logf("Starting to generate %v directories with %v files in each with %v file per commit", dirsCount, totalFiles/dirsCount, filesPerCommit)

	// Use same content for all files (doesn't matter for this benchmark)
	fileContent := []byte("Benchmark test file content\n")

	// Create directories first
	for dir := 0; dir < dirsCount; dir++ {
		dirName := fmt.Sprintf("dir%02d", dir)
		dirPath := filepath.Join(templatePath, dirName)
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			b.Fatal(err)
		}
	}

	// Parallelize file creation with a worker pool
	type fileJob struct {
		dir  int
		file int
	}
	
	jobs := make(chan fileJob, totalFiles)
	errors := make(chan error, 10)
	
	// Start 10 worker goroutines
	const numWorkers = 10
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				dirName := fmt.Sprintf("dir%02d", job.dir)
				fileName := fmt.Sprintf("file%04d.txt", job.file)
				filePath := filepath.Join(templatePath, dirName, fileName)
				
				if err := os.WriteFile(filePath, fileContent, 0644); err != nil {
					select {
					case errors <- err:
					default:
					}
					return
				}
			}
		}()
	}
	
	// Send all file creation jobs
	for dir := 0; dir < dirsCount; dir++ {
		for file := 0; file < filesPerDir; file++ {
			jobs <- fileJob{dir: dir, file: file}
		}
	}
	close(jobs)
	
	// Wait for all workers to complete
	wg.Wait()
	close(errors)
	
	// Check for any errors
	if err := <-errors; err != nil {
		b.Fatal(err)
	}
	
	b.Logf("File creation complete, starting git add operations...")

	fileCount := 0
	commitCount := 0

	// Pre-allocate array for batch processing
	filesToAdd := make([]string, 0, filesPerCommit)

	for dir := 0; dir < dirsCount; dir++ {
		dirName := fmt.Sprintf("dir%02d", dir)

		for file := 0; file < filesPerDir; file++ {
			// Collect files for batch add
			fileName := fmt.Sprintf("file%04d.txt", file)
			relPath := filepath.Join(dirName, fileName)
			filesToAdd = append(filesToAdd, relPath)
			fileCount++

			// Batch add and commit every filesPerCommit files
			if len(filesToAdd) >= filesPerCommit {
				// Get status ONCE for the entire batch
				s, err := templateWorkTree.Status()
				if err != nil {
					b.Fatal(err)
				}

				// Get index ONCE for the entire batch
				idx, err := templateRepo.Storer.Index()
				if err != nil {
					b.Fatal(err)
				}

				// Add all files using the same status and index
				var saveIndex bool
				for _, path := range filesToAdd {
					added, _, err := templateWorkTree.doAddFile(idx, s, path, make([]gitignore.Pattern, 0))
					if err != nil {
						b.Fatal(err)
					}
					if added {
						saveIndex = true
					}
				}

				// Save index if any files were added
				if saveIndex {
					if err := templateRepo.Storer.SetIndex(idx); err != nil {
						b.Fatal(err)
					}
				}

				commitCount++
				commitMsg := fmt.Sprintf("Add batch %d (%d files total)", commitCount, fileCount)
				_, err = templateWorkTree.Commit(commitMsg, &CommitOptions{
					Author: &object.Signature{
						Name:  "Benchmark",
						Email: "bench@example.com",
					},
				})
				if err != nil {
					b.Fatal(err)
				}
				b.Logf("Add batch %d (%v files total)", commitCount, fileCount)

				// Reset batch array (reuse capacity)
				filesToAdd = filesToAdd[:0]
			}
		}
	}

	// Handle remaining files if any
	if len(filesToAdd) > 0 {
		// Get status ONCE for the remaining batch
		s, err := templateWorkTree.Status()
		if err != nil {
			b.Fatal(err)
		}

		// Get index ONCE for the remaining batch
		idx, err := templateRepo.Storer.Index()
		if err != nil {
			b.Fatal(err)
		}

		// Add all remaining files
		var saveIndex bool
		for _, path := range filesToAdd {
			added, _, err := templateWorkTree.doAddFile(idx, s, path, make([]gitignore.Pattern, 0))
			if err != nil {
				b.Fatal(err)
			}
			if added {
				saveIndex = true
			}
		}

		// Save index if any files were added
		if saveIndex {
			if err := templateRepo.Storer.SetIndex(idx); err != nil {
				b.Fatal(err)
			}
		}

		commitCount++
		commitMsg := fmt.Sprintf("Add batch %d (%d files total)", commitCount, fileCount)
		_, err = templateWorkTree.Commit(commitMsg, &CommitOptions{
			Author: &object.Signature{
				Name:  "Benchmark",
				Email: "bench@example.com",
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		b.Logf("Add batch %d (%v files total)", commitCount, fileCount)
	}

	setupDuration := b.Elapsed() - setupStart
	b.Logf("Template setup complete: %d files, %d commits in %v", fileCount, commitCount, setupDuration)

	// Phase 2: Clone template with Shared: true (sets up alternates automatically)
	b.Log("Cloning template with Shared: true (alternates)...")
	cloneStart := b.Elapsed()

	workRepo, err := PlainClone(workPath, &CloneOptions{
		URL:    templatePath,
		Shared: true, // This automatically sets up alternates!
	})
	if err != nil {
		b.Fatal(err)
	}

	cloneDuration := b.Elapsed() - cloneStart
	b.Logf("Clone completed in %v (using alternates)", cloneDuration)

	// Exit here to profile just the initialization phase
	b.Logf("Total setup time: %v", b.Elapsed())

	// Verify workRepo is valid before exiting
	if workRepo == nil {
		b.Fatal("workRepo is nil")
	}

	b.SkipNow()
}
