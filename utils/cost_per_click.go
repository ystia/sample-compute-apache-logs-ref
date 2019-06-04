package utils

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"

	"github.com/ystia/sample-compute-apache-logs-ref/types"
)

func LoadCostPerClick(configPath string) (types.CostPerClick, error) {

	var costPerClick types.CostPerClick

	if configPath == "" {
		configPath = "cost_per_click.yml"
	}

	costPerClickFile, err := ioutil.ReadFile(configPath)

	if err != nil {
		return nil, errors.Wrap(err, "Unable to read file where cost per click is defined")
	}

	err = yaml.Unmarshal(costPerClickFile, &costPerClick)

	if err != nil {
		return nil, errors.Wrap(err, "Unable to decode cost per click file")
	}

	return costPerClick, nil

}
