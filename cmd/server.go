package cmd

import (
	cloudconfig "cloudsidecar/pkg/config"
	"cloudsidecar/pkg/logging"
	"cloudsidecar/pkg/server"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
	"strings"
)

var config *cloudconfig.Config
var change chan string

var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "Run cloud cloudsidecar",
	Long:  `Run cloud cloudsidecar`,
	Run: func(cmd *cobra.Command, args []string) {
		server.Main(config, change, cmd, args)
	},
}

var configFile string
var configDir string
var versionFlag bool
var watcher *fsnotify.Watcher

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "", "config file")
	rootCmd.PersistentFlags().StringVar(&configDir, "config-dir", "", "config directory")
	rootCmd.PersistentFlags().BoolVar(&versionFlag, "version", false, "display version")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func readInConfigsFromDirectory() error {
	i := 0
	var err error
	if watcher != nil {
		if err = watcher.Close(); err != nil {
			logging.Log.Error("Close failure", err)
			return err
		}
	}
	watcher, err = fsnotify.NewWatcher()
	if err != nil {
		logging.Log.Error("Watch error", err)
		return err
	}
	if watcherErr := watcher.Add(configDir); watcherErr != nil {
		return watcherErr
	}
	err = filepath.Walk(configDir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && isConfigFile(info.Name()) {
			logging.Log.Info("Loading config file %s", path)
			viper.SetConfigFile(path)
			if i == 0 {
				i++
				return viper.ReadInConfig()
			} else {
				i++
				return viper.MergeInConfig()
			}
		} else {
			return nil
		}
	})
	go watchMultipleFiles()
	return err
}

func isConfigFile(filename string) bool {
	return strings.HasSuffix(filename, ".yaml") || strings.HasSuffix(filename, ".json")

}

func watchMultipleFiles() {
	select {
	case e, ok := <-watcher.Events:
		if ok {
			if err := readInConfigsFromDirectory(); err != nil {
				logging.Log.Error("Error reloading", err)
			}
			if isConfigFile(e.Name) {
				change <- e.Name
			}
			return
		}
	case err, _ := <-watcher.Errors:
		logging.Log.Errorf("Error watching", err)
		return
	}
	return
}

func initConfig() {
	config = &cloudconfig.Config{}
	change = make(chan string)
	logging.Init("", "")
	if versionFlag {
		fmt.Println(server.Version)
		os.Exit(0)
	}
	var err error
	if configFile != "" {
		viper.SetConfigFile(configFile)
		err = viper.ReadInConfig()
		viper.WatchConfig()
		viper.OnConfigChange(func(e fsnotify.Event) {
			change <- e.Name
		})
	} else if configDir != "" {
		err = readInConfigsFromDirectory()
	} else {
		panic("--config or --config-dir required")
	}
	if err != nil {
		logging.Log.Error("", err)
		panic(fmt.Sprintf("Cannot load config %s %s", os.Args[1], err))
	}
}
