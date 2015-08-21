package main

import (
	"flag"
	"io/ioutil"
	"math"
	"os"

	"github.com/olivere/elastic"

	"gitlab.neoway.com.br/severino/logger"

	"code.google.com/p/go-uuid/uuid"
)

const defaultElasticSearch = "http://127.0.0.1:9200"

var (
	fromHost       string
	fromIndex      string
	toHost         string
	toIndex        string
	newMapping     string
	mappingContent string
	bulkSize       int
)

func main() {
	flag.StringVar(&fromHost, "from-host", defaultElasticSearch, "elastic search host to get data from")
	flag.StringVar(&fromIndex, "index", "", "name of index to reindex/copy")
	flag.StringVar(&toHost, "to-host", defaultElasticSearch, "elastic search host to get data from")
	flag.StringVar(&toIndex, "new-index", "", "name of new-index")
	flag.StringVar(&newMapping, "new-mapping", "", "path to new mapping file of new-index")
	flag.IntVar(&bulkSize, "bulk-size", 500, "amount of data to get in each request")

	flag.Parse()

	if fromIndex == "" {
		logger.Error("The `-index` parameters are required to reindex/copy")

		flag.Usage()
		os.Exit(1)
	}

	// Read new mapping file
	if newMapping != "" {
		mappingBytes, err := ioutil.ReadFile(newMapping)
		if err != nil {
			logger.Fatal("Error reading mapping file: %+v", err.Error())
		}

		mappingContent = string(mappingBytes)
		logger.Debug("New mapping of %s:", newMapping)
		logger.Debug(mappingContent)
	}

	// Set default toIndex
	if toIndex == "" {
		toIndex = fromIndex + "-" + uuid.New()[24:]
	}

	// Connect to clients
	fromClient, err := getESClient(fromHost)
	if err != nil {
		logger.Fatal("Error connecting to `%s`: %+v", fromHost, err.Error())
	}

	toClient, err := getESClient(toHost)
	if err != nil {
		logger.Fatal("Error connecting to `%s`: %+v", toHost, err.Error())
	}

	// Verify if fromIndex exists
	exists, err := fromClient.IndexExists(fromIndex).Do()
	if err != nil {
		logger.Fatal("Error verifying if index <%s> exists: %+v", fromIndex, err.Error())
	}
	if !exists {
		logger.Fatal("The index <%s> doesn't exists, we need a valid index or alias", fromIndex)
	}

	// Verify if toIndex already exists
	exists, err = toClient.IndexExists(toIndex).Do()
	if err != nil {
		logger.Fatal("Error verifying if index <%s> exists: %+v", toIndex, err.Error())
	}

	// If toIndex don't exists we need create it
	if !exists {
		indexService := toClient.CreateIndex(toIndex)
		// If -new-mapping was not provided use original mapping
		if newMapping == "" {
			mapping, err := fromClient.GetMapping().Index(fromIndex).Do()
			if err != nil {
				logger.Fatal("Error getting mapping of index <%s>", fromIndex)
			}

			indexService.BodyJson(mapping)
		} else {
			indexService.BodyString(mappingContent)
		}

		createNewIndex, err := indexService.Do()
		if err != nil {
			logger.Fatal("Error creating new index <%s>: %+v", toIndex, err.Error())
		}
		if !createNewIndex.Acknowledged {
			logger.Fatal("Was not possible create new index <%s>", toIndex)
		}

		logger.Info("New index <%s> was created!", toIndex)
	}

	// Reindex fromIndex to toIndex
	reindexer := fromClient.Reindex(fromIndex, toIndex)
	reindexer.TargetClient(toClient)
	reindexer.Progress(showReindexProgress)

	if bulkSize > 0 {
		reindexer.BulkSize(bulkSize)
	}

	resp, err := reindexer.Do()
	if err != nil {
		logger.Fatal("Error trying reindexing: %+v", err.Error())
	}

	logger.Info("Reindexed was completed %d documents successed and %d failed", resp.Success, resp.Failed)

	// If index is a alias, update its reference
	aliasesService := toClient.Aliases()
	aliases, err := aliasesService.Do()
	if err != nil {
		logger.Fatal("Error getting aliases: %+v", err.Error())
	}

	indices := aliases.IndicesByAlias(fromIndex)
	if len(indices) > 0 {
		aliasService := toClient.Alias()
		for _, index := range indices {
			aliasService.Remove(index, fromIndex)
		}
		_, err = aliasService.Add(toIndex, fromIndex).Do()
		if err != nil {
			logger.Fatal("Error updating alias <%s>:  %+v", fromIndex, err.Error())
		}

		logger.Info("Alias <%s>: %+v was removed and now point to: <%s>", fromIndex, indices, toIndex)
	}
}

func getESClient(esURL string) (*elastic.Client, error) {
	esClient, err := elastic.NewClient(
		elastic.SetURL(esURL),
		elastic.SetSniff(false),
		elastic.SetErrorLog(logger.DefaultLogger.Handlers[0].(*logger.DefaultHandler).ErrorLogger),
		elastic.SetInfoLog(logger.DefaultLogger.Handlers[0].(*logger.DefaultHandler).DebugLogger),
		elastic.SetTraceLog(logger.DefaultLogger.Handlers[0].(*logger.DefaultHandler).DebugLogger),
	)

	if err != nil {
		return esClient, err
	}

	esVersion, err := esClient.ElasticsearchVersion(esURL)
	if err != nil {
		logger.Fatal("Error getting ES version: %+v", err.Error())
	}
	logger.Info("Connected in Elasticsearch <%s>, version %s", esURL, esVersion)

	return esClient, err
}

func showReindexProgress(current, total int64) {
	percent := (float64(current) / float64(total)) * 100
	if int64(percent) == int64(math.Ceil(percent)) {
		logger.Info("Reindexing... %d%%", int64(percent))
	}
}
