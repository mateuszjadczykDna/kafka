package main

import (
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/performance"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/soak_clients"
	"github.com/spf13/cobra"
	"os"
	"strconv"
)

var (
	// Injected from linker flag like `go build -ldflags "-X main.version=$VERSION"`
	version = "0.0.0"

	cli = &cobra.Command{
		Use:   "cc-services-cli",
		Short: "cc-services-cli",
	}

	performanceTestsCmd = &cobra.Command{
		Use:   "performance-tests action",
		Short: "performance tests",
		Run:   performanceTest,
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

	topicConfigPath        = os.Getenv("TROGDOR_TOPIC_CONFIG_PATH")
	testConfigPath         = os.Getenv("PERFORMANCE_TEST_CONFIG_PATH")
	trogdorCoordinatorHost = os.Getenv("TROGDOR_HOST")
	trogdorAgentsCount, _  = strconv.Atoi(os.Getenv("TROGDOR_AGENTS_COUNT"))
	bootstrapServers       = os.Getenv("TROGDOR_BOOTSTRAPSERVERS")
)

func init() {
	cli.Version = version

	cli.AddCommand(soakClientsCmd)
	soakClientsCmd.AddCommand(clientsReportCmd)
	soakClientsCmd.AddCommand(spawnClientsCmd)
	cli.AddCommand(performanceTestsCmd)
}

func performanceTest(cmd *cobra.Command, agrs []string) {
	performance.Run(testConfigPath, trogdorCoordinatorHost, trogdorAgentsCount, bootstrapServers)
}

func spawnClients(cmd *cobra.Command, args []string) {
	soak_clients.Run(topicConfigPath, trogdorCoordinatorHost, trogdorAgentsCount, bootstrapServers)
}

func clientsReport(cmd *cobra.Command, args []string) {
	soak_clients.Report(topicConfigPath, trogdorCoordinatorHost)
}

func main() {
	if err := cli.Execute(); err != nil {
		panic(err)
	}
}
