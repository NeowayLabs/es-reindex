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

const elasticSearchAddr = "http://127.0.0.1:9200"

var (
	index       string
	mappingFile string
	newIndex    string
)

func main() {
	flag.StringVar(&index, "index", "", "name of index to reindex")
	flag.StringVar(&mappingFile, "mapping-file", "", "path from mapping file of new index")
	flag.StringVar(&newIndex, "new-index", "", "name of new index with data reindexed, default will be <index-name>-<uuid>")

	flag.Parse()

	if index == "" || mappingFile == "" {
		logger.Error("The parameters -index and -mapping-file are required to reindex")

		flag.Usage()
		os.Exit(1)
	}

	mappingBytes, err := ioutil.ReadFile(mappingFile)
	if err != nil {
		logger.Fatal("Error reading mapping file: %+v", err.Error())
	}

	mappingContent := string(mappingBytes)
	logger.Debug("Mapping content:")
	logger.Debug(mappingContent)
	logger.Debug("")

	if newIndex == "" {
		newIndex = index + "-" + uuid.New()[24:]
	}

	esClient, err := elastic.NewClient(
		elastic.SetURL(elasticSearchAddr),
		elastic.SetSniff(false),
		elastic.SetErrorLog(logger.DefaultLogger.Handlers[0].(*logger.DefaultHandler).ErrorLogger),
		elastic.SetInfoLog(logger.DefaultLogger.Handlers[0].(*logger.DefaultHandler).InfoLogger),
		elastic.SetTraceLog(logger.DefaultLogger.Handlers[0].(*logger.DefaultHandler).DebugLogger),
	)
	if err != nil {
		logger.Fatal("Error connecting to ES: %+v", err.Error())
	}

	// Verify if index exists
	exists, err := esClient.IndexExists(index).Do()
	if err != nil {
		logger.Fatal("Error verifying if index exists: %+v", err.Error())
	}
	if !exists {
		logger.Fatal("The index <%s> doesn't exists, pass an index or alias already created", index)
	}

	// Verify if newIndex already exists
	exists, err = esClient.IndexExists(newIndex).Do()
	if err != nil {
		logger.Fatal("Error verifying if newIndex exists: %+v", err.Error())
	}
	if exists {
		logger.Fatal("The newIndex <%s> already exists, pass the name of NEW index", newIndex)
	}

	// Get Elastic Search version
	esVersion, err := esClient.ElasticsearchVersion(elasticSearchAddr)
	if err != nil {
		logger.Fatal("Error getting ES version: %+v", err.Error())
	}
	logger.Info("Elasticsearch version %s", esVersion)

	// Create newIndex using mapping-file content
	createNewIndex, err := esClient.CreateIndex(newIndex).Body(mappingContent).Do()
	if err != nil {
		logger.Fatal("Error creating newIndex: %+v", err.Error())
	}
	if !createNewIndex.Acknowledged {
		logger.Fatal("Was not possible create new index <%s>", newIndex)
	}
	logger.Info("New index <%s> was created!", newIndex)

	// Reindex index to newIndex
	reindexer := esClient.Reindex(index, newIndex)
	reindexer.Progress(showReindexProgress)

	resp, err := reindexer.Do()
	if err != nil {
		logger.Fatal("Error trying reindexing: %+v", err.Error())
	}

	logger.Info("Reindexed was completed %d documents successed and %d failed", resp.Success, resp.Failed)

	// If index is a alias, update its reference
	aliasesService := esClient.Aliases()
	aliases, err := aliasesService.Do()
	if err != nil {
		logger.Fatal("Error getting aliases: %+v", err.Error())
	}

	indices := aliases.IndicesByAlias(index)
	if len(indices) > 0 {
		aliasService := esClient.Alias()
		for _, v := range indices {
			aliasService = aliasService.Remove(v, index)
		}
		_, err = aliasService.Add(newIndex, index).Do()
		if err != nil {
			logger.Fatal("Error updating aliases:  %+v", err.Error())
		}

		logger.Info("As <%s> is a alias, %+v was removed and alias now point to: <%s>", index, indices, newIndex)
	}
}

func showReindexProgress(current, total int64) {
	percent := (float64(current) / float64(total)) * 100
	if int64(percent) == int64(math.Ceil(percent)) {
		logger.Info("Reindexing... %d%%", int64(percent))
	}
}
