package types

type ReferrerFromLog struct {
	ReferrerDomain string `json:"referrer_domain"`
}

type SponsoredLinkPrice struct {
	ReferrerDomain string `json:"referrer_domain"`
	Price float64 `json:"price"`
}

type CostPerClick map[string]float64