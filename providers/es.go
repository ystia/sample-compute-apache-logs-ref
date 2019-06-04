package providers

import (
	"context"
	"encoding/json"
	"math"
	"regexp"
	"strings"
	"sync"

	"github.com/olivere/elastic"
	"github.com/pkg/errors"

	"github.com/ystia/sample-compute-apache-logs-ref/types"
)

const (
	SPONSORED_LINK_INDEX = "sponsored_link"
	SPONSORED_LINK_TYPE  = "price"

	LOGSTASH_DOC_TYPE = "doc"

	MAX_RESULT_WINDOW = 10000
)

type ElasticsearchClient struct {
	Ctx    context.Context // A context.Context object that is required by the Elasticsearch client in order to operate requests.
	Client *elastic.Client // The Elasticsearch Client that will be used to operate the requests.
	mux    sync.RWMutex    // A lock that will help to avoid failure on getting and saving datasets
}

func NewElasticsearchClient(esNodes string) (*ElasticsearchClient, error) {

	e := new(ElasticsearchClient)

	isValidEsNodes := false

	// Starting with elastic.v5, you must pass a context to execute each service
	e.Ctx = context.Background()

	// Ensure the ES nodes given in parameter respect the format.

	r, err := regexp.Compile(`^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9]):[0-9]+` +
		`(,(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9]):[0-9]+)*$`)

	if err != nil {
		return nil, err
	}

	if r.MatchString(esNodes) {
		isValidEsNodes = true
	}

	if isValidEsNodes == false {
		r, err := regexp.Compile(`^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]):[0-9]+` +
			`(,(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]):[0-9]+)$`)

		if err != nil {
			return nil, err
		}

		if !r.MatchString(esNodes) {
			return nil, errors.New("The format of the the 'esNodes' argument is not correct.")
		}
	}

	esNodesSlice := strings.Split(esNodes, ",")

	var esURLs []string

	for _, esNode := range esNodesSlice {
		esURLs = append(esURLs, "http://"+esNode)
	}

	client, err := elastic.NewClient(elastic.SetURL(esURLs...), elastic.SetSniff(false))
	if err != nil {
		// Handle error
		return nil, err
	}

	e.Client = client

	return e, nil
}

func (ec *ElasticsearchClient) getNumberOfDocuments(esIndex string, esType string) (int64, error) {

	if esIndex == "" {
		return -1, errors.New("The index value is empty.")
	}

	if esType == "" {
		return -1, errors.New("The document type is empty.")
	}

	getItems, err := ec.Client.Search().
		Index(esIndex).
		Type(esType).
		From(0).
		Size(1).
		Do(ec.Ctx)

	if err != nil {
		return -1, errors.Wrapf(err, "Unable to query Elasticsearch to get the number of %s documents from index %s", SPONSORED_LINK_TYPE, SPONSORED_LINK_INDEX)
	}

	return getItems.Hits.TotalHits, nil
}

func (ec *ElasticsearchClient) getLogstashIndicesList() ([]string, error) {

	var logstashIndicesList []string
	re := regexp.MustCompile("logstash-.*")

	globalIndicesList, err := ec.Client.IndexNames()

	if err != nil {
		return nil, errors.Wrapf(err, "Unable to get the list of Elasticsearch indices")
	}

	for _, index := range globalIndicesList {
		if re.MatchString(index) {
			logstashIndicesList = append(logstashIndicesList, index)
		}
	}

	return logstashIndicesList, nil

}

func (ec *ElasticsearchClient) GetReferrerDomainsFromLogs() ([]types.ReferrerFromLog, error) {

	var referrers []types.ReferrerFromLog

	ec.mux.RLock()
	defer ec.mux.RUnlock()

	logstashIndices, err := ec.getLogstashIndicesList()

	if err != nil {
		return nil, errors.Wrap(err, "Unable to get the list of logstash indices")
	}

	for _, logstashIndex := range logstashIndices {

		nbDocuments, err := ec.getNumberOfDocuments(logstashIndex, LOGSTASH_DOC_TYPE)

		if err != nil {
			return nil, errors.Wrap(err, "Unable to get the number of logs stored in Elasticsearch")
		}

		for ; nbDocuments > 0; nbDocuments -= MAX_RESULT_WINDOW {

			getLogstashDocument, err := ec.Client.Search().
				Index(logstashIndex).
				Type(LOGSTASH_DOC_TYPE).
				From(0).
				Size(int(math.Min(float64(MAX_RESULT_WINDOW), float64(nbDocuments)))).
				Do(ec.Ctx)

			if err != nil {
				return nil, errors.Wrap(err, "Unable to query Elasticsearch to get all logs")
			}

			if getLogstashDocument.Hits.TotalHits == 0 {
				// No logs have been returned. Returning nil value without error
				return nil, nil
			}

			for _, hit := range getLogstashDocument.Hits.Hits {

				var referrer types.ReferrerFromLog

				err := json.Unmarshal(*hit.Source, &referrer)

				if err != nil {
					return nil, err
				}

				referrers = append(referrers, referrer)
			}
		}
	}

	return referrers, nil
}

func (ec *ElasticsearchClient) SaveSponsoredLinkPrices(sponsoredLinkPrices []types.SponsoredLinkPrice) error {

	for _, sponsoredLinkPrice := range sponsoredLinkPrices {
		if sponsoredLinkPrice.ReferrerDomain == "" {
			return errors.Errorf("Unable to save a SponsoredLinkPrice '%v' with a non refered domain.", sponsoredLinkPrice)
		}

		if sponsoredLinkPrice.Price < 0 {
			return errors.Errorf("Unable to save a SponsoredLinkPrice '%v' with price inferior to 0.", sponsoredLinkPrice)
		}
	}

	// We'll remove the index if it already exists in order to have one instance of values.

	indexNames, err := ec.Client.IndexNames()

	if err != nil {
		return errors.Wrap(err, "Unable to retrieve index list")
	}

	for _, index := range indexNames {
		if index == SPONSORED_LINK_INDEX {
			deleteResponse, err := ec.Client.DeleteIndex(SPONSORED_LINK_INDEX).Do(ec.Ctx)

			if err != nil {
				return errors.Wrapf(err, "Unable to delete index '%s'", SPONSORED_LINK_INDEX)
			}

			if !deleteResponse.Acknowledged {
				return errors.New("Deleting index hasn't ben acknoledged.")
			}

			break
		}
	}

	bulkRequest := ec.Client.Bulk()

	for _, sponsoredLinkPrice := range sponsoredLinkPrices {
		bulkRequest.Add(elastic.NewBulkIndexRequest().
			Index(SPONSORED_LINK_INDEX).
			Type(SPONSORED_LINK_TYPE).
			Doc(sponsoredLinkPrice))
	}

	_, err = bulkRequest.Do(ec.Ctx)

	if err != nil {
		return errors.Wrap(err, "Unable to index SponsoredLinkPrices into Elasticsearch")
	}

	_, err = ec.Client.Flush().Index(SPONSORED_LINK_INDEX).Do(ec.Ctx)

	if err != nil {
		return errors.Wrap(err, "Unable to flush Elasticsearch index")
	}

	return nil
}

func (ec *ElasticsearchClient) SaveSponsoredLinkPrice(slp types.SponsoredLinkPrice) error {

	if slp.ReferrerDomain == "" {
		return errors.New("Unable to save a SponsoredLinkPrice with a non refered domain.")
	}

	if slp.Price < 0 {
		return errors.New("Unable to save a SponsoredLinkPrice with price inferior to 0.")
	}

	// We'll remove the index if it already exists in order to have one instance of values.

	indexNames, err := ec.Client.IndexNames()

	if err != nil {
		return errors.Wrap(err, "Unable to retrieve index list")
	}

	for _, index := range indexNames {
		if index == SPONSORED_LINK_INDEX {
			deleteResponse, err := ec.Client.DeleteIndex(SPONSORED_LINK_INDEX).Do(ec.Ctx)

			if err != nil {
				return errors.Wrapf(err, "Unable to delete index '%s'", SPONSORED_LINK_INDEX)
			}

			if !deleteResponse.Acknowledged {
				return errors.New("Deleting index hasn't ben acknoledged.")
			}

			break
		}
	}

	_, err = ec.Client.Index().
		Index(SPONSORED_LINK_INDEX).
		Type(SPONSORED_LINK_TYPE).
		BodyJson(slp).
		Do(ec.Ctx)

	if err != nil {
		return errors.Wrap(err, "Unable to index SponsoredLinkPrice into Elasticsearch")
	}

	_, err = ec.Client.Flush().Index(SPONSORED_LINK_INDEX).Do(ec.Ctx)

	if err != nil {
		return errors.Wrap(err, "Unable to flush Elasticserach index")
	}

	return nil

}
