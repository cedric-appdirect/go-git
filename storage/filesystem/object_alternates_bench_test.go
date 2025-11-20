package filesystem_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// BenchmarkAlternatesPerformance benchmarks git operations with alternates.
// This simulates a real-world scenario where a template repository with many files
// is cloned using alternates, then new commits are made and pushed.
//
// Setup:
//   - Creates a "template" repository with 40,000 files in 10 directories
//   - Files are committed in batches (40 commits of 1,000 files each)
//   - Creates a "work" repository using alternates to reference the template
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
	tempDir, err := ioutil.TempDir("", "git-alternates-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	templatePath := filepath.Join(tempDir, "template")
	workPath := filepath.Join(tempDir, "work")

	// Phase 1: Create template repository with 40,000 files
	b.Log("Setting up template repository with 40,000 files...")
	setupStart := b.Elapsed()
	
	templateRepo, err := git.PlainInit(templatePath, false)
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
		totalFiles      = 40000
		dirsCount       = 10
		filesPerDir     = totalFiles / dirsCount
		filesPerCommit  = 1000
	)

	fileCount := 0
	commitCount := 0
	
	for dir := 0; dir < dirsCount; dir++ {
		dirName := fmt.Sprintf("dir%02d", dir)
		dirPath := filepath.Join(templatePath, dirName)
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			b.Fatal(err)
		}

		for file := 0; file < filesPerDir; file++ {
			fileName := fmt.Sprintf("file%04d.txt", file)
			filePath := filepath.Join(dirPath, fileName)
			content := fmt.Sprintf("Content for file %d in directory %d\n", file, dir)
			
			if err := ioutil.WriteFile(filePath, []byte(content), 0644); err != nil {
				b.Fatal(err)
			}

			// Add to git
			relPath := filepath.Join(dirName, fileName)
			if _, err := templateWorkTree.Add(relPath); err != nil {
				b.Fatal(err)
			}

			fileCount++

			// Commit every filesPerCommit files
			if fileCount%filesPerCommit == 0 {
				commitCount++
				commitMsg := fmt.Sprintf("Add batch %d (%d files total)", commitCount, fileCount)
				_, err := templateWorkTree.Commit(commitMsg, &git.CommitOptions{
					Author: &object.Signature{
						Name:  "Benchmark",
						Email: "bench@example.com",
					},
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	setupDuration := b.Elapsed() - setupStart
	b.Logf("Template setup complete: %d files, %d commits in %v", fileCount, commitCount, setupDuration)

	// Phase 2: Create work repository with alternates
	if err := os.MkdirAll(workPath, 0755); err != nil {
		b.Fatal(err)
	}

	workRepo, err := git.PlainInit(workPath, false)
	if err != nil {
		b.Fatal(err)
	}

	// Set up alternates manually
	workGitDir := filepath.Join(workPath, ".git")
	alternatesFile := filepath.Join(workGitDir, "objects", "info", "alternates")
	
	if err := os.MkdirAll(filepath.Dir(alternatesFile), 0755); err != nil {
		b.Fatal(err)
	}

	templateObjectsPath := filepath.Join(templatePath, ".git", "objects")
	if err := ioutil.WriteFile(alternatesFile, []byte(templateObjectsPath+"\n"), 0644); err != nil {
		b.Fatal(err)
	}

	// Copy refs from template
	templateRefsPath := filepath.Join(templatePath, ".git", "refs")
	workRefsPath := filepath.Join(workGitDir, "refs")
	if err := copyDir(templateRefsPath, workRefsPath); err != nil {
		b.Fatal(err)
	}

	// Copy HEAD
	templateHEAD, err := ioutil.ReadFile(filepath.Join(templatePath, ".git", "HEAD"))
	if err != nil {
		b.Fatal(err)
	}
	if err := ioutil.WriteFile(filepath.Join(workGitDir, "HEAD"), templateHEAD, 0644); err != nil {
		b.Fatal(err)
	}

	// Get worktree
	workTree, err := workRepo.Worktree()
	if err != nil {
		b.Fatal(err)
	}

	// Checkout to populate working directory (this tests alternates!)
	b.Log("Checking out working directory (testing alternates)...")
	checkoutStart := b.Elapsed()
	err = workTree.Checkout(&git.CheckoutOptions{
		Force: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	checkoutDuration := b.Elapsed() - checkoutStart
	b.Logf("Checkout completed in %v (all objects from alternates)", checkoutDuration)

	// Reset timer before benchmarking actual operations
	b.ResetTimer()

	// Benchmark: Commit new files (triggers object enumeration through alternates)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		
		// Add a new file
		newFile := filepath.Join(workPath, fmt.Sprintf("bench_file_%d.txt", i))
		content := fmt.Sprintf("Benchmark iteration %d\n", i)
		if err := ioutil.WriteFile(newFile, []byte(content), 0644); err != nil {
			b.Fatal(err)
		}

		if _, err := workTree.Add(filepath.Base(newFile)); err != nil {
			b.Fatal(err)
		}

		b.StartTimer()

		// Commit (this triggers object lookups through alternates)
		_, err := workTree.Commit(fmt.Sprintf("Benchmark commit %d", i), &git.CommitOptions{
			Author: &object.Signature{
				Name:  "Benchmark",
				Email: "bench@example.com",
			},
		})
		if err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
	}
}

// BenchmarkAlternatesObjectLookup benchmarks repeated object lookups through alternates.
// This directly measures the caching improvement for the common case of looking up
// the same objects multiple times.
func BenchmarkAlternatesObjectLookup(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	// Create a simple setup with alternates
	tempDir, err := ioutil.TempDir("", "git-lookup-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	templatePath := filepath.Join(tempDir, "template")
	workPath := filepath.Join(tempDir, "work")

	// Create template repository
	templateRepo, err := git.PlainInit(templatePath, false)
	if err != nil {
		b.Fatal(err)
	}

	wt, err := templateRepo.Worktree()
	if err != nil {
		b.Fatal(err)
	}

	// Create 1000 files
	for i := 0; i < 1000; i++ {
		fileName := filepath.Join(templatePath, fmt.Sprintf("file%04d.txt", i))
		if err := ioutil.WriteFile(fileName, []byte(fmt.Sprintf("content %d\n", i)), 0644); err != nil {
			b.Fatal(err)
		}
		wt.Add(filepath.Base(fileName))
	}

	// Commit
	_, err = wt.Commit("Initial commit", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Benchmark",
			Email: "bench@example.com",
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	// Create work repo with alternates
	workRepo, err := git.PlainInit(workPath, false)
	if err != nil {
		b.Fatal(err)
	}

	// Set up alternates
	workGitDir := filepath.Join(workPath, ".git")
	alternatesFile := filepath.Join(workGitDir, "objects", "info", "alternates")
	if err := os.MkdirAll(filepath.Dir(alternatesFile), 0755); err != nil {
		b.Fatal(err)
	}

	templateObjectsPath := filepath.Join(templatePath, ".git", "objects")
	if err := ioutil.WriteFile(alternatesFile, []byte(templateObjectsPath+"\n"), 0644); err != nil {
		b.Fatal(err)
	}

	// Copy refs
	templateRefsPath := filepath.Join(templatePath, ".git", "refs")
	workRefsPath := filepath.Join(workGitDir, "refs")
	if err := copyDir(templateRefsPath, workRefsPath); err != nil {
		b.Fatal(err)
	}

	// Copy HEAD
	templateHEAD, err := ioutil.ReadFile(filepath.Join(templatePath, ".git", "HEAD"))
	if err != nil {
		b.Fatal(err)
	}
	if err := ioutil.WriteFile(filepath.Join(workGitDir, "HEAD"), templateHEAD, 0644); err != nil {
		b.Fatal(err)
	}

	// Get commit hashes to look up
	ref, err := workRepo.Head()
	if err != nil {
		b.Fatal(err)
	}

	commits, err := workRepo.Log(&git.LogOptions{From: ref.Hash()})
	if err != nil {
		b.Fatal(err)
	}

	var hashes []plumbing.Hash
	commits.ForEach(func(c *object.Commit) error {
		hashes = append(hashes, c.Hash)
		return nil
	})
	commits.Close()

	if len(hashes) == 0 {
		b.Fatal("No commits found")
	}

	b.Logf("Testing lookups with %d commit hashes", len(hashes))
	b.ResetTimer()

	// Benchmark repeated lookups - this tests the caching effect
	for i := 0; i < b.N; i++ {
		hash := hashes[i%len(hashes)]
		_, err := workRepo.CommitObject(hash)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// copyDir recursively copies a directory
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Get relative path
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		// Copy file
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		return ioutil.WriteFile(dstPath, data, info.Mode())
	})
}
