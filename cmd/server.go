package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"sidecar/pkg/logging"
	"sidecar/pkg/server"
)

var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "Run cloud sidecar",
	Long: `Run cloud sidecar`,
	Run: server.Main,
}

var configFile string

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "", "config file")
}


func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func initConfig() {
	logging.Init()
	viper.SetConfigFile(configFile)
	err := viper.ReadInConfig()
	if err != nil {
		logging.Log.Error("", err)
		panic(fmt.Sprintf("Cannot load config %s %s", os.Args[1], err))
	}
}


