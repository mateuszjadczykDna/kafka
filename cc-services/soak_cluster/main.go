package main

import (
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/soak_clients"
	"github.com/spf13/cobra"
	"os"
)

var (
	// Injected from linker flag like `go build -ldflags "-X main.version=$VERSION"`
	version = "0.0.0"

	cli = &cobra.Command{
		Use:   "cc-services-cli",
		Short: "cc-services-cli",
	}

	soakClientsCmd = &cobra.Command{
		Use:   "soak-clients action",
		Short: "soak clients",
	}

	clientsReportCmd = &cobra.Command{
		Use:   "report",
		Short: "reports the soak cluster's clients status",
		Run:   clientsReport,
	}

	spawnClientsCmd = &cobra.Command{
		Use:   "spawn",
		Short: "spawns soak cluster clients",
		Run:   spawnClients,
	}

	topicConfigPath = os.Getenv("TROGDOR_TOPIC_CONFIG_PATH")
)

func init() {
	cli.Version = version

	cli.AddCommand(soakClientsCmd)
	soakClientsCmd.AddCommand(clientsReportCmd)
	soakClientsCmd.AddCommand(spawnClientsCmd)
}

func spawnClients(cmd *cobra.Command, args []string) {
	soak_clients.Run(topicConfigPath)
}

func clientsReport(cmd *cobra.Command, args []string) {
	soak_clients.Report(topicConfigPath)
}

func main() {
	if err := cli.Execute(); err != nil {
		panic(err)
	}
}
