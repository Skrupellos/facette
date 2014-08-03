package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/facette/facette/pkg/config"
	"github.com/facette/facette/pkg/connector"
	"github.com/facette/facette/pkg/library"
	"github.com/facette/facette/pkg/logger"
	"github.com/facette/facette/pkg/plot"
	"github.com/facette/facette/pkg/utils"
	"github.com/facette/facette/thirdparty/github.com/fatih/set"
)

func (server *Server) serveGraph(writer http.ResponseWriter, request *http.Request) {
	if request.Method != "GET" && request.Method != "HEAD" && server.Config.ReadOnly {
		server.serveResponse(writer, serverResponse{mesgReadOnlyMode}, http.StatusForbidden)
		return
	}

	graphID := strings.TrimPrefix(request.URL.Path, urlLibraryPath+"graphs/")

	switch request.Method {
	case "DELETE":
		if graphID == "" {
			server.serveResponse(writer, serverResponse{mesgMethodNotAllowed}, http.StatusMethodNotAllowed)
			return
		}

		err := server.Library.DeleteItem(graphID, library.LibraryItemGraph)
		if os.IsNotExist(err) {
			server.serveResponse(writer, serverResponse{mesgResourceNotFound}, http.StatusNotFound)
			return
		} else if err != nil {
			logger.Log(logger.LevelError, "server", "%s", err)
			server.serveResponse(writer, serverResponse{mesgUnhandledError}, http.StatusInternalServerError)
			return
		}

		server.serveResponse(writer, nil, http.StatusOK)

	case "GET", "HEAD":
		if graphID == "" {
			server.serveGraphList(writer, request)
			return
		}

		item, err := server.Library.GetItem(graphID, library.LibraryItemGraph)
		if os.IsNotExist(err) {
			server.serveResponse(writer, serverResponse{mesgResourceNotFound}, http.StatusNotFound)
			return
		} else if err != nil {
			logger.Log(logger.LevelError, "server", "%s", err)
			server.serveResponse(writer, serverResponse{mesgUnhandledError}, http.StatusInternalServerError)
			return
		}

		server.serveResponse(writer, item, http.StatusOK)

	case "POST", "PUT":
		var graph *library.Graph

		if response, status := server.parseStoreRequest(writer, request, graphID); status != http.StatusOK {
			server.serveResponse(writer, response, status)
			return
		}

		if request.Method == "POST" && request.FormValue("inherit") != "" {
			// Get graph from library
			item, err := server.Library.GetItem(request.FormValue("inherit"), library.LibraryItemGraph)
			if os.IsNotExist(err) {
				server.serveResponse(writer, serverResponse{mesgResourceNotFound}, http.StatusNotFound)
				return
			} else if err != nil {
				logger.Log(logger.LevelError, "server", "%s", err)
				server.serveResponse(writer, serverResponse{mesgUnhandledError}, http.StatusInternalServerError)
				return
			}

			graph = &library.Graph{}
			utils.Clone(item.(*library.Graph), graph)

			graph.ID = ""
		} else {
			// Create a new graph instance
			graph = &library.Graph{Item: library.Item{ID: graphID}}
		}

		graph.Modified = time.Now()

		// Parse input JSON for graph data
		body, _ := ioutil.ReadAll(request.Body)

		if err := json.Unmarshal(body, graph); err != nil {
			logger.Log(logger.LevelError, "server", "%s", err)
			server.serveResponse(writer, serverResponse{mesgResourceInvalid}, http.StatusBadRequest)
			return
		}

		err := server.Library.StoreItem(graph, library.LibraryItemGraph)
		if response, status := server.parseError(writer, request, err); status != http.StatusOK {
			logger.Log(logger.LevelError, "server", "%s", err)
			server.serveResponse(writer, response, status)
			return
		}

		if request.Method == "POST" {
			writer.Header().Add("Location", strings.TrimRight(request.URL.Path, "/")+"/"+graph.ID)
			server.serveResponse(writer, nil, http.StatusCreated)
		} else {
			server.serveResponse(writer, nil, http.StatusOK)
		}

	default:
		server.serveResponse(writer, serverResponse{mesgMethodNotAllowed}, http.StatusMethodNotAllowed)
	}
}

func (server *Server) serveGraphList(writer http.ResponseWriter, request *http.Request) {
	var (
		items         ItemListResponse
		offset, limit int
	)

	if response, status := server.parseListRequest(writer, request, &offset, &limit); status != http.StatusOK {
		server.serveResponse(writer, response, status)
		return
	}

	graphSet := set.New(set.ThreadSafe)

	// Filter on collection if any
	if request.FormValue("collection") != "" {
		item, err := server.Library.GetItem(request.FormValue("collection"), library.LibraryItemCollection)
		if os.IsNotExist(err) {
			server.serveResponse(writer, serverResponse{mesgResourceNotFound}, http.StatusNotFound)
			return
		} else if err != nil {
			logger.Log(logger.LevelError, "server", "%s", err)
			server.serveResponse(writer, serverResponse{mesgUnhandledError}, http.StatusInternalServerError)
			return
		}

		collection := item.(*library.Collection)

		for _, graph := range collection.Entries {
			graphSet.Add(graph.ID)
		}
	}

	// Fill graphs list
	items = make(ItemListResponse, 0)

	for _, graph := range server.Library.Graphs {
		if !graphSet.IsEmpty() && !graphSet.Has(graph.ID) {
			continue
		}

		if request.FormValue("filter") != "" && !utils.FilterMatch(request.FormValue("filter"), graph.Name) {
			continue
		}

		items = append(items, &ItemResponse{
			ID:          graph.ID,
			Name:        graph.Name,
			Description: graph.Description,
			Modified:    graph.Modified.Format(time.RFC3339),
		})
	}

	response := &listResponse{
		list:   items,
		offset: offset,
		limit:  limit,
	}

	server.applyResponseLimit(writer, request, response)

	server.serveResponse(writer, response.list, http.StatusOK)
}

func (server *Server) serveGraphPlots(writer http.ResponseWriter, request *http.Request) {
	var (
		err   error
		graph *library.Graph
		item  interface{}
	)

	if request.Method != "POST" && request.Method != "HEAD" {
		server.serveResponse(writer, serverResponse{mesgMethodNotAllowed}, http.StatusMethodNotAllowed)
		return
	} else if utils.HTTPGetContentType(request) != "application/json" {
		server.serveResponse(writer, serverResponse{mesgUnsupportedMediaType}, http.StatusUnsupportedMediaType)
		return
	}

	// Parse plots request
	plotReq, err := parsePlotRequest(request)
	if err != nil {
		logger.Log(logger.LevelError, "server", "%s", err)
		server.serveResponse(writer, serverResponse{mesgResourceInvalid}, http.StatusBadRequest)
		return
	}

	// Get graph from library
	graph = plotReq.Graph

	if plotReq.ID != "" {
		if item, err = server.Library.GetItem(plotReq.ID, library.LibraryItemGraph); err == nil {
			graph = item.(*library.Graph)
		}
	}

	if graph == nil {
		err = os.ErrNotExist
	}

	// Prepare queries
	providerQueries, err := server.prepareProviderQueries(plotReq, graph)

	// Stop if an error was encountered
	if err != nil {
		if os.IsNotExist(err) {
			server.serveResponse(writer, serverResponse{mesgResourceNotFound}, http.StatusNotFound)
		} else {
			logger.Log(logger.LevelError, "server", "%s", err)
			server.serveResponse(writer, serverResponse{mesgUnhandledError}, http.StatusInternalServerError)
		}

		return
	}

	// Execute queries
	plotSeries := make(map[string]plot.Series)

	for _, providerQuery := range providerQueries {
		plots, err := providerQuery.connector.GetPlots(&providerQuery.query)
		if err != nil {
			logger.Log(logger.LevelError, "server", "%s", err)
		}

		for plotsIndex, plotsItem := range plots {
			plotSeries[providerQuery.queryMap[plotsIndex]] = plotsItem
		}
	}

	if len(plotSeries) == 0 {
		server.serveResponse(writer, serverResponse{mesgEmptyData}, http.StatusOK)
		return
	}

	response := &PlotResponse{
		ID:          graph.ID,
		Start:       plotReq.startTime.Format(time.RFC3339),
		End:         plotReq.endTime.Format(time.RFC3339),
		Name:        graph.Name,
		Description: graph.Description,
		Type:        graph.Type,
		StackMode:   graph.StackMode,
		UnitType:    graph.UnitType,
		UnitLegend:  graph.UnitLegend,
		Modified:    graph.Modified,
	}

	for _, groupItem := range graph.Groups {
		groupSeries := make([]plot.Series, len(groupItem.Series))

		for seriesIndex, seriesItem := range groupItem.Series {
			if _, ok := plotSeries[seriesItem.Name]; !ok {
				logger.Log(logger.LevelError, "server", "unable to find plots for `%s' series", seriesItem.Name)
				continue
			}

			groupSeries[seriesIndex] = plotSeries[seriesItem.Name]

			// Apply series scale if any
			if scale, _ := config.GetFloat(seriesItem.Options, "scale", false); scale != 0 {
				groupSeries[seriesIndex].Scale(plot.Value(scale))
			}
		}

		// Handle series operations
		if groupItem.Type == plot.OperTypeAverage || groupItem.Type == plot.OperTypeSum {
			consolidatedSeries, err := plot.ConsolidateSeries(
				groupSeries,
				plotReq.startTime,
				plotReq.endTime,
				plotReq.Sample,
				plot.ConsolidateAverage,
			)
			if err != nil {
				logger.Log(logger.LevelError, "server", "unable to consolidate series: %s", err)
				continue
			}

			if groupItem.Type == plot.OperTypeAverage {
				operSeries, err := plot.AverageSeries(consolidatedSeries)
				if err != nil {
					logger.Log(logger.LevelError, "server", "unable to average series: %s", err)
					continue
				}

				groupSeries = []plot.Series{operSeries}
			} else {
				operSeries, err := plot.SumSeries(consolidatedSeries)
				if err != nil {
					logger.Log(logger.LevelError, "server", "unable to average series: %s", err)
					continue
				}

				groupSeries = []plot.Series{operSeries}
			}

			// Apply group scale if any
			if scale, _ := config.GetFloat(groupItem.Options, "scale", false); scale != 0 {
				groupSeries[0].Scale(plot.Value(scale))
			}
		} else {
			// Downsample single series
			for _, seriesItem := range groupSeries {
				seriesItem.Downsample(plotReq.startTime, plotReq.endTime, plotReq.Sample, plot.ConsolidateAverage)
			}
		}

		for _, seriesItem := range groupSeries {
			seriesItem.Summarize(plotReq.Percentiles)

			response.Series = append(response.Series, &SeriesResponse{
				Name:    seriesItem.Name,
				Plots:   seriesItem.Plots,
				Summary: seriesItem.Summary,
				Options: groupItem.Options,
			})
		}
	}

	server.serveResponse(writer, response, http.StatusOK)
}

func (server *Server) prepareProviderQueries(plotReq *PlotRequest,
	graph *library.Graph) (map[string]*providerQuery, error) {

	providerQueries := make(map[string]*providerQuery)

	for _, groupItem := range graph.Groups {
		for _, seriesItem := range groupItem.Series {
			var seriesSources []string

			// Check for missing origins
			if _, ok := server.Catalog.Origins[seriesItem.Origin]; !ok {
				return nil, fmt.Errorf("unknown series origin `%s'", seriesItem.Origin)
			}

			// Handle sources/source groups
			if strings.HasPrefix(seriesItem.Source, library.LibraryGroupPrefix) {
				seriesSources = server.Library.ExpandGroup(
					strings.TrimPrefix(seriesItem.Source, library.LibraryGroupPrefix),
					library.LibraryItemSourceGroup,
				)
			} else {
				seriesSources = []string{seriesItem.Source}
			}

			// Handle metrics/metric groups
			for _, sourceItem := range seriesSources {
				var seriesMetrics []string

				if strings.HasPrefix(seriesItem.Metric, library.LibraryGroupPrefix) {
					seriesMetrics = server.Library.ExpandGroup(
						strings.TrimPrefix(seriesItem.Metric, library.LibraryGroupPrefix),
						library.LibraryItemMetricGroup,
					)
				} else {
					seriesMetrics = []string{seriesItem.Metric}
				}

				for _, metricItem := range seriesMetrics {
					// Get series metric
					metric := server.Catalog.GetMetric(seriesItem.Origin, sourceItem, metricItem)

					if metric == nil {
						logger.Log(logger.LevelWarning, "server", "unknown metric `%s' for source `%s' (origin: %s)",
							metricItem, sourceItem, seriesItem.Origin)

						continue
					}

					// Get provider name
					providerName := metric.Connector.(connector.Connector).GetName()

					// Initialize provider query if needed
					if _, ok := providerQueries[providerName]; !ok {
						providerQueries[providerName] = &providerQuery{
							query: plot.Query{
								StartTime: plotReq.startTime,
								EndTime:   plotReq.endTime,
								Sample:    plotReq.Sample,
								Metrics:   make([]plot.QueryMetric, 0),
							},
							queryMap:  make([]string, 0),
							connector: metric.Connector.(connector.Connector),
						}
					}

					// Append metric to provider query
					providerQueries[providerName].query.Metrics = append(
						providerQueries[providerName].query.Metrics,
						plot.QueryMetric{
							Name:   metric.OriginalName,
							Origin: metric.Source.Origin.OriginalName,
							Source: metric.Source.OriginalName,
						},
					)

					providerQueries[providerName].queryMap = append(
						providerQueries[providerName].queryMap,
						seriesItem.Name,
					)
				}
			}
		}
	}

	return providerQueries, nil
}

func parsePlotRequest(request *http.Request) (*PlotRequest, error) {
	var err error

	plotReq := &PlotRequest{}

	// Parse input JSON for plots request
	body, _ := ioutil.ReadAll(request.Body)
	if err = json.Unmarshal(body, plotReq); err != nil {
		return nil, err
	}

	// Check plots request parameters
	if plotReq.Time.IsZero() {
		plotReq.endTime = time.Now()
	} else if strings.HasPrefix(strings.Trim(plotReq.Range, " "), "-") {
		plotReq.endTime = plotReq.Time
	} else {
		plotReq.startTime = plotReq.Time
	}

	if plotReq.startTime.IsZero() {
		if plotReq.startTime, err = utils.TimeApplyRange(plotReq.endTime, plotReq.Range); err != nil {
			return nil, err
		}
	} else if plotReq.endTime, err = utils.TimeApplyRange(plotReq.startTime, plotReq.Range); err != nil {
		return nil, err
	}

	if plotReq.Sample == 0 {
		plotReq.Sample = config.DefaultPlotSample
	}

	return plotReq, nil
}
