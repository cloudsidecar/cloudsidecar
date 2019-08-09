package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"cloudsidecar/pkg/logging"
	"cloudsidecar/pkg/server"
)

var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "Run cloud cloudsidecar",
	Long: `Run cloud cloudsidecar`,
	Run: server.Main,
}


var configFile string
var versionFlag bool

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "", "config file")
	rootCmd.PersistentFlags().BoolVar(&versionFlag, "version", false, "display version")
}


func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func initConfig() {
	logging.Init("", "")
	if versionFlag {
		fmt.Println(server.Version)
		os.Exit(0)
	}
	viper.SetConfigFile(configFile)
	err := viper.ReadInConfig()
	if err != nil {
		logging.Log.Error("", err)
		panic(fmt.Sprintf("Cannot load config %s %s", os.Args[1], err))
	}
}


