package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/urfave/cli"

	"github.com/ystia/sample-compute-apache-logs-ref/providers"
	"github.com/ystia/sample-compute-apache-logs-ref/types"
	"github.com/ystia/sample-compute-apache-logs-ref/utils"
)

func main() {

	app := cli.NewApp()
	app.Name = "Compute the price per domain from apache log generator."
	app.Version = "0.0.1"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "es_nodes",
			Usage:  "Elasticsearch nodes to connect to. Expected format is: \"<es_node1>:<es_port1>[,<es_node2>:<es_port2>...]\"",
			EnvVar: "CC_ES_NODES",
		},
		cli.StringFlag{
			Name:   "c, config",
			Usage:  "Path to the cost_per_click.yml file used to compute global sponsored link cost.",
			EnvVar: "CC_CONFIG",
		},
	}

	app.Action = func(c *cli.Context) error {
		requestsPerReferrer := map[string]int{}
		var sponsoredLinkPrices []types.SponsoredLinkPrice

		// Instantiate Elasticsearch connexion
		fmt.Printf("\nConnecting to Elasticsearch using nodes %s...", c.String("es_nodes"))
		ec, err := providers.NewElasticsearchClient(c.String("es_nodes"))

		if err != nil {
			return err
		}

		fmt.Println("  OK")

		// Get logs referrer domain name from Elasticsearch

		fmt.Print("Retrieving logs from Elasticsearch...")

		referrers, err := ec.GetReferrerDomainsFromLogs()

		if err != nil {
			return errors.Errorf("Unable to get refferer domains from log: %v\n", err)
		}

		for _, referrer := range referrers {

			// We only get the last domain.com part of the domain. So that www.smith.com is the same than smith.com
			domainNameSplitted := strings.Split(referrer.ReferrerDomain, ".")
			baseDomainName := domainNameSplitted[len(domainNameSplitted)-2] + "." + domainNameSplitted[len(domainNameSplitted)-1]

			requestsPerReferrer[baseDomainName] = requestsPerReferrer[baseDomainName] + 1
		}

		var totalNbOfRequest int

		for _, nbRequest := range requestsPerReferrer {
			totalNbOfRequest += nbRequest
		}

		if totalNbOfRequest != len(referrers) {
			return errors.New("Some requests has not been taken into account. We won't save result on Elasticsearch. Exiting...")
		}

		fmt.Println("  OK")
		fmt.Println("  " + strconv.Itoa(totalNbOfRequest) + " log(s) haven been retrieved.")

		// Compute price for each domain

		fmt.Printf("Computing price for each domain with config file %s...", c.String("config"))

		costsPerClick, err := utils.LoadCostPerClick(c.String("config"))

		if err != nil {
			errors.Errorf("Unable to load cost per click from file: %v\n", err)
		}

		var priceComputedCounter int
		for baseDomainName, nbRequests := range requestsPerReferrer {

			for costPerClickDName, costPerClick := range costsPerClick {
				if baseDomainName == costPerClickDName {

					totalPrice := float64(nbRequests) * costPerClick

					sponsoredLinkPrices = append(sponsoredLinkPrices, types.SponsoredLinkPrice{
						ReferrerDomain: baseDomainName,
						Price:          math.Ceil(totalPrice*100) / 100,
					})

					priceComputedCounter++
					break
				}
			}
		}

		fmt.Println("  OK")
		fmt.Println("  " + strconv.Itoa(priceComputedCounter) + " sponsored link cost(s) have been computed.")

		// Save the result in a new index in Elasticsearch

		fmt.Print("Saving results on elasticsearch...")

		err = ec.SaveSponsoredLinkPrices(sponsoredLinkPrices)

		if err != nil {
			errors.Errorf("Unable to save sponsored links in Elasticsearch: %v\n", err)
		}

		fmt.Println("  OK")

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println("  FAILED")
		fmt.Println(err)
		os.Exit(1)
	}
}
