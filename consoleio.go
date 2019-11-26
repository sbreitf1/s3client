package main

import (
	"fmt"
	"strings"

	"github.com/sbreitf1/go-console"
)

var (
	colorWarning   = "\033[1;31m"
	colorHighlight = "\033[1;31m"
	colorTarget    = "\033[1;32m"
	colorPrefix    = "\033[1;34m"
	colorEnd       = "\033[0m"
)

func prepareCLE() *console.CommandLineEnvironment {
	cle := console.NewCommandLineEnvironment("")
	cle.SetPrompt(func() string {
		if len(currentBucket) > 0 {
			if len(currentPrefix) > 0 {
				return fmt.Sprintf(colorTarget+"{%s@%s}"+colorEnd+colorPrefix+"%s"+colorEnd, currentBucket, currentTarget.Key, currentPrefix)
			}
			return fmt.Sprintf(colorTarget+"{%s@%s}"+colorEnd, currentBucket, currentTarget.Key)
		}
		return fmt.Sprintf(colorTarget+"{%s}"+colorEnd, currentTarget.Key)
	})

	cle.RegisterCommand(console.NewExitCommand("exit"))
	cle.RegisterCommand(console.NewParameterlessCommand("help", help))
	cle.RegisterCommand(console.NewCustomCommand("enter", newArgsCompletion(newArgBucket()), enter))
	cle.RegisterCommand(console.NewParameterlessCommand("leave", leave))
	cle.RegisterCommand(console.NewCustomCommand("cd", newArgsCompletion(newArgRemoteFile(false)), cd))
	cle.RegisterCommand(console.NewCustomCommand("ls", newArgsCompletion(newArgRemoteFile(false)), ls))
	cle.RegisterCommand(console.NewCustomCommand("rm", newArgsCompletion(newArgRemoteFile(true)), rm))
	cle.RegisterCommand(console.NewCustomCommand("dl", newArgsCompletion(newArgRemoteFile(true), newArgLocalFile(true)), dl))
	cle.RegisterCommand(console.NewCustomCommand("ul", newArgsCompletion(newArgLocalFile(true), newArgRemoteFile(true)), dl))
	cle.RegisterCommand(console.NewCustomCommand("mv", newArgsCompletion(newArgRemoteFile(true), newArgRemoteFile(true)), mv))
	cle.RegisterCommand(console.NewCustomCommand("cp", newArgsCompletion(newArgRemoteFile(true), newArgRemoteFile(true)), cp))
	cle.RegisterCommand(console.NewCustomCommand("touch", newArgsCompletion(newArgRemoteFile(true)), touch))
	cle.RegisterCommand(console.NewCustomCommand("cat", newArgsCompletion(newArgRemoteFile(true)), cat))
	cle.RegisterCommand(console.NewCustomCommand("find", newArgsCompletion(nil, newArgRemoteFile(false)), find))
	cle.RegisterCommand(console.NewCustomCommand("list", newArgsCompletion(newArgOneOf("bucket", "env")), list))
	cle.RegisterCommand(console.NewCustomCommand("mkbucket", nil, mkbucket))
	cle.RegisterCommand(console.NewCustomCommand("rmbucket", newArgsCompletion(newArgBucket()), rmbucket))

	return cle
}

func runCLE() error {
	cle := prepareCLE()
	return cle.Run()
}

/* ################################################ */
/* ###              arg completion              ### */
/* ################################################ */

type argsCompletion struct {
	args []argCompletion
}

func newArgsCompletion(args ...argCompletion) console.CompletionCandidatesForEntry {
	return (&argsCompletion{args}).GetCandidates
}

func (a *argsCompletion) GetCandidates(currentCommand []string, entryIndex int) []console.CompletionCandidate {
	if entryIndex >= 1 && entryIndex <= len(a.args) {
		if a.args[entryIndex-1] != nil {
			return a.args[entryIndex-1].GetCandidates(currentCommand, entryIndex)
		}
	}
	return nil
}

type argCompletion interface {
	GetCandidates(currentCommand []string, entryIndex int) []console.CompletionCandidate
}

type argOneOf struct {
	candidates []console.CompletionCandidate
}

func newArgOneOf(list ...string) *argOneOf {
	return &argOneOf{stringsToCandidates(list, true)}
}

func (a *argOneOf) GetCandidates(currentCommand []string, entryIndex int) []console.CompletionCandidate {
	return a.candidates
}

type argBucket struct{}

func newArgBucket() *argBucket {
	return &argBucket{}
}

func (a *argBucket) GetCandidates(currentCommand []string, entryIndex int) []console.CompletionCandidate {
	buckets, err := getBuckets()
	if err == nil {
		return stringsToCandidates(buckets, true)
	}
	return nil
}

type argRemoteFile struct {
	withFiles bool
}

func newArgRemoteFile(withFiles bool) *argRemoteFile {
	return &argRemoteFile{withFiles}
}

func (a *argRemoteFile) GetCandidates(currentCommand []string, entryIndex int) []console.CompletionCandidate {
	files, err := getRemoteFiles(currentPrefix + currentCommand[entryIndex])
	if err == nil {
		candidates := make([]console.CompletionCandidate, 0)
		for i := range files {
			isDir := strings.HasSuffix(files[i], "/")
			if a.withFiles || isDir {
				candidates = append(candidates, console.CompletionCandidate{ReplaceString: files[i][len(currentPrefix):], IsFinal: !isDir})
			}
		}
		return candidates
	}
	return nil
}

type argLocalFile struct {
	withFiles bool
}

func newArgLocalFile(withFiles bool) *argLocalFile {
	return &argLocalFile{withFiles}
}

func (a *argLocalFile) GetCandidates(currentCommand []string, entryIndex int) []console.CompletionCandidate {
	candidates, _ := console.BrowseCandidates("", currentCommand[entryIndex], a.withFiles)
	return candidates
}

func stringsToCandidates(list []string, isFinal bool) []console.CompletionCandidate {
	candidates := make([]console.CompletionCandidate, len(list))
	for i := range list {
		candidates[i] = console.CompletionCandidate{ReplaceString: list[i], IsFinal: isFinal}
	}
	return candidates
}

/* ################################################ */
/* ###              read wrappers               ### */
/* ################################################ */

func readln() (string, error) {
	return console.ReadLine()
}

func readlnNonEmpty() (string, error) {
	line, err := readln()
	if err != nil {
		return "", err
	}
	if len(line) == 0 {
		return "", errUserAbort{}
	}
	return line, nil
}

func readpw() (string, error) {
	return console.ReadPassword()
}

func readpwNonEmpty() (string, error) {
	line, err := readpw()
	if err != nil {
		return "", err
	}
	if len(line) == 0 {
		return "", errUserAbort{}
	}
	return line, nil
}

func println(a ...interface{}) {
	console.Println(a...)
}

func printlnf(format string, args ...interface{}) {
	console.Printlnf(format, args...)
}

type errUserAbort struct{}

func (errUserAbort) Error() string { return "aborted by user" }
