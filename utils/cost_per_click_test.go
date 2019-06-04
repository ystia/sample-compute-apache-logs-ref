package utils

import "testing"

func TestLoadCostPerClick(t *testing.T) {

	costPerClick, err := LoadCostPerClick("../cost_per_click.yml")

	if err != nil {
		t.Fatalf("Unable to load cost per click from file: %v", err)
	}

	if len(costPerClick) == 0 {
		t.Fatalf("CostPerClick object is empty")
	}

}
