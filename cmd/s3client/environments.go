package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"regexp"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/sbreitf1/fs"
)

func getConfigDir() (string, error) {
	usr, err := user.Current()
	if err != nil {
		return "", err
	}
	return path.Join(usr.HomeDir, ".s3client"), nil
}

func getEnvironments() ([]S3Target, error) {
	configDir, err := getConfigDir()
	if err != nil {
		return nil, err
	}

	files, err := ioutil.ReadDir(configDir)
	if err != nil {
		if os.IsNotExist(err) {
			return make([]S3Target, 0), nil
		}
		return nil, err
	}

	environments := make([]S3Target, 0)
	for _, f := range files {
		if !f.IsDir() {
			target, err := readEnv(path.Join(configDir, f.Name()))
			if err != nil {
				println("WARN: failed to load environment %q: %s", f.Name(), err.Error())
			}

			environments = append(environments, target)
		}
	}

	return environments, nil
}

func checkEnvKey(key string) error {
	pattern := regexp.MustCompile("^[a-zA-Z0-9_\\- ]+$")
	if !pattern.MatchString(key) {
		return fmt.Errorf("the environment key contains invalid characters")
	}
	return nil
}

func prepareEnv(key string) (S3Target, error) {
	if len(key) == 0 {
		return selectEnv()
	}

	if err := checkEnvKey(key); err != nil {
		return S3Target{}, err
	}

	target, err := loadOrCreateEnv(key)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}

	return target, nil
}

func selectEnv() (S3Target, error) {
	environments, err := getEnvironments()
	if err != nil {
		return S3Target{}, err
	}

	if len(environments) == 0 {
		return S3Target{}, fmt.Errorf("no environments saved yet. Please use \"-e {name}\" to create a new environment and use it")
	}
	if len(environments) == 1 {
		return environments[0], nil
	}

	// find max key len to better align the options:
	maxKeyLen := 0
	for _, e := range environments {
		if len(e.Key) > maxKeyLen {
			maxKeyLen = len(e.Key)
		}
	}

	promptList := make([]string, len(environments))
	for i := range environments {
		padding := strings.Repeat(" ", maxKeyLen-len(environments[i].Key))
		promptList[i] = fmt.Sprintf("%s%s  ->  %s", environments[i].Key, padding, environments[i].Endpoint)
	}
	ui := promptui.Select{Label: "Select environment or option", Items: promptList}
	index, _, err := ui.Run()
	if err != nil {
		return S3Target{}, err
	}

	return environments[index], nil
}

func loadOrCreateEnv(key string) (S3Target, error) {
	configDir, err := getConfigDir()
	if err != nil {
		return S3Target{}, err
	}

	filePath := path.Join(configDir, key+".json")
	target, err := readEnv(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return newEnv(key, filePath)
		}
		return S3Target{}, err
	}

	return target, nil
}

func readEnv(filePath string) (S3Target, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return S3Target{}, err
	}

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return S3Target{}, fmt.Errorf("unable to read environment file: %s", err.Error())
	}

	var target S3Target
	if err := json.Unmarshal(data, &target); err != nil {
		return S3Target{}, fmt.Errorf("malformed environment file: %s", err.Error())
	}

	target.SourceFile = filePath
	return target, nil
}

func newEnv(key string, filePath string) (S3Target, error) {
	println("The environment %q does not exist and will be created:", key)
	target, err := enterTarget(key)
	if err != nil {
		return S3Target{}, err
	}

	data, err := json.Marshal(&target)
	if err != nil {
		return S3Target{}, err
	}

	isDir, err := fs.IsDir(path.Dir(filePath))
	if err != nil {
		return S3Target{}, err
	}

	if !isDir {
		if err := fs.CreateDirectory(path.Dir(filePath)); err != nil {
			return S3Target{}, err
		}
	}

	if err := ioutil.WriteFile(filePath, data, os.ModePerm); err != nil {
		return S3Target{}, err
	}

	target.SourceFile = filePath
	return target, nil
}

func enterTarget(key string) (S3Target, error) {
	fmt.Print("URL> ")
	url, err := readlnNonEmpty()
	if err != nil {
		return S3Target{}, err
	}

	var secure bool
	if strings.HasPrefix(strings.ToLower(url), "http://") {
		url = url[7:]
		secure = false
	} else if strings.HasPrefix(strings.ToLower(url), "https://") {
		url = url[8:]
		secure = true
	} else {
		fmt.Print("Secure (yes/no)?> ")
		str, err := readlnNonEmpty()
		if err != nil {
			return S3Target{}, err
		}

		secure = (str[0] == 'y' || str[0] == 'Y')
	}

	fmt.Print("Access Key> ")
	accessKey, err := readlnNonEmpty()
	if err != nil {
		return S3Target{}, err
	}

	fmt.Print("Secret Key> ")
	secretKey, err := readlnNonEmpty()
	if err != nil {
		return S3Target{}, err
	}

	return S3Target{Key: key, Endpoint: url, Secure: secure, AccessKey: accessKey, SecretKey: secretKey}, nil
}
