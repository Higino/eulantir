package cmd

import (
	"fmt"
	"strings"

	"github.com/higino/eulantir/internal/connector"

	// import built-in connectors so their init() functions register them
	_ "github.com/higino/eulantir/internal/connectors/csv"
	_ "github.com/higino/eulantir/internal/connectors/postgres"

	"github.com/spf13/cobra"
)

var connectorsCmd = &cobra.Command{
	Use:   "connectors",
	Short: "Manage and inspect available connectors",
}

var connectorsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all registered connectors",
	RunE: func(cmd *cobra.Command, args []string) error {
		list := connector.Default.List()
		fmt.Printf("%-12s  %s\n", "TYPE", "DESCRIPTION")
		fmt.Printf("%-12s  %s\n", strings.Repeat("-", 12), strings.Repeat("-", 40))
		for _, info := range list {
			fmt.Printf("%-12s  %s\n", info.Type, info.Description)
			fmt.Printf("%-12s  config: %s\n", "", strings.Join(info.ConfigKeys, ", "))
		}
		return nil
	},
}

var connectorsInfoCmd = &cobra.Command{
	Use:   "info <connector-type>",
	Short: "Show config schema for a connector type",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		typ := args[0]
		list := connector.Default.List()
		for _, info := range list {
			if info.Type == typ {
				fmt.Printf("Type:        %s\n", info.Type)
				fmt.Printf("Description: %s\n", info.Description)
				fmt.Printf("Config keys: %s\n", strings.Join(info.ConfigKeys, ", "))
				return nil
			}
		}
		return fmt.Errorf("unknown connector type %q — run 'eulantir connectors list' to see available types", typ)
	},
}

func init() {
	connectorsCmd.AddCommand(connectorsListCmd)
	connectorsCmd.AddCommand(connectorsInfoCmd)
}
