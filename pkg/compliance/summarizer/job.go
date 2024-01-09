package summarizer

import (
	"context"
	"strings"

	"github.com/kaytu-io/kaytu-engine/pkg/auth/api"
	"github.com/kaytu-io/kaytu-engine/pkg/compliance/es"
	types2 "github.com/kaytu-io/kaytu-engine/pkg/compliance/summarizer/types"
	"github.com/kaytu-io/kaytu-engine/pkg/httpclient"
	inventoryApi "github.com/kaytu-io/kaytu-engine/pkg/inventory/api"
	onboardApi "github.com/kaytu-io/kaytu-engine/pkg/onboard/api"
	"github.com/kaytu-io/kaytu-engine/pkg/types"
	es2 "github.com/kaytu-io/kaytu-util/pkg/es"
	"github.com/kaytu-io/kaytu-util/pkg/pipeline"
	"go.uber.org/zap"
)

func (w *Worker) RunJob(j types2.Job) error {
	ctx := context.Background()

	w.logger.Info("Running summarizer",
		zap.Uint("job_id", j.ID),
		zap.String("benchmark_id", j.BenchmarkID),
	)

	// We have to sort by kaytuResourceID to be able to optimize memory usage for resourceFinding generations
	// this way as soon as paginator switches to next resource we can send the previous resource to the queue and free up memory
	paginator, err := es.NewFindingPaginator(w.esClient, types.FindingsIndex, nil, nil, []map[string]any{{"kaytuResourceID": "asc"}})
	if err != nil {
		return err
	}

	w.logger.Info("FindingsIndex paginator ready")

	jd := types2.JobDocs{
		BenchmarkSummary: types2.BenchmarkSummary{
			BenchmarkID:      j.BenchmarkID,
			JobID:            j.ID,
			EvaluatedAtEpoch: j.CreatedAt.Unix(),
			Connections: types2.BenchmarkSummaryResult{
				BenchmarkResult: types2.ResultGroup{
					Result: types2.Result{
						QueryResult:    map[types.ConformanceStatus]int{},
						SeverityResult: map[types.FindingSeverity]int{},
						SecurityScore:  0,
					},
					ResourceTypes: map[string]types2.Result{},
					Controls:      map[string]types2.ControlResult{},
				},
				Connections: map[string]types2.ResultGroup{},
			},
			ResourceCollections: map[string]types2.BenchmarkSummaryResult{},
		},
		ResourcesFindings:       make(map[string]types.ResourceFinding),
		ResourcesFindingsIsDone: make(map[string]bool),

		ResourceCollectionCache: map[string]inventoryApi.ResourceCollection{},
		ConnectionCache:         map[string]onboardApi.Connection{},
	}

	resourceCollections, err := w.inventoryClient.ListResourceCollections(&httpclient.Context{UserRole: api.InternalRole})
	if err != nil {
		w.logger.Error("failed to list resource collections", zap.Error(err))
		return err
	}
	for _, rc := range resourceCollections {
		rc := rc
		jd.ResourceCollectionCache[rc.ID] = rc
	}

	connections, err := w.onboardClient.ListSources(&httpclient.Context{UserRole: api.InternalRole}, nil)
	if err != nil {
		w.logger.Error("failed to list connections", zap.Error(err))
		return err
	}
	for _, c := range connections {
		c := c
		// use provider id instead of kaytu id because we need that to check resource collections
		jd.ConnectionCache[strings.ToLower(c.ConnectionID)] = c
	}

	for page := 1; paginator.HasNext(); page++ {
		w.logger.Info("Next page", zap.Int("page", page))
		page, err := paginator.NextPage(ctx)
		if err != nil {
			w.logger.Error("failed to fetch next page", zap.Error(err))
			return err
		}

		resourceIds := make([]string, 0, len(page))
		for _, f := range page {
			resourceIds = append(resourceIds, f.KaytuResourceID)
		}

		lookupResources, err := es.FetchLookupByResourceIDBatch(w.esClient, resourceIds)
		if err != nil {
			w.logger.Error("failed to fetch lookup resources", zap.Error(err))
			return err
		}
		lookupResourcesMap := make(map[string]*es2.LookupResource)
		for _, r := range lookupResources {
			r := r
			lookupResourcesMap[r.ResourceID] = &r
		}

		w.logger.Info("page size", zap.Int("pageSize", len(page)))
		for _, f := range page {
			jd.AddFinding(w.logger, j, f, lookupResourcesMap[f.KaytuResourceID])
		}

		var docs []es2.Doc
		for resourceId, isReady := range jd.ResourcesFindingsIsDone {
			if !isReady {
				continue
			}
			resourceFinding := jd.SummarizeResourceFinding(w.logger, jd.ResourcesFindings[resourceId])
			keys, idx := resourceFinding.KeysAndIndex()
			resourceFinding.EsID = es2.HashOf(keys...)
			resourceFinding.EsIndex = idx
			docs = append(docs, resourceFinding)
			delete(jd.ResourcesFindings, resourceId)
			delete(jd.ResourcesFindingsIsDone, resourceId)
		}
		w.logger.Info("Sending resource finding docs", zap.Int("docCount", len(docs)))

		if err := pipeline.SendToPipeline(w.config.ElasticSearch.IngestionEndpoint, docs); err != nil {
			w.logger.Error("failed to send to pipeline", zap.Error(err))
			return err
		}
	}

	err = paginator.Close(ctx)
	if err != nil {
		return err
	}

	w.logger.Info("Starting to summarizer",
		zap.Uint("job_id", j.ID),
		zap.String("benchmark_id", j.BenchmarkID),
	)

	jd.Summarize(w.logger)

	w.logger.Info("Summarize done", zap.Any("summary", jd))

	keys, idx := jd.BenchmarkSummary.KeysAndIndex()
	jd.BenchmarkSummary.EsID = es2.HashOf(keys...)
	jd.BenchmarkSummary.EsIndex = idx

	docs := make([]es2.Doc, 0, len(jd.ResourcesFindings)+1)
	docs = append(docs, jd.BenchmarkSummary)
	resourceIds := make([]string, 0, len(jd.ResourcesFindings))
	for resourceId, rf := range jd.ResourcesFindings {
		resourceIds = append(resourceIds, resourceId)
		keys, idx := rf.KeysAndIndex()
		rf.EsID = es2.HashOf(keys...)
		rf.EsIndex = idx
		docs = append(docs, rf)
	}
	if err := pipeline.SendToPipeline(w.config.ElasticSearch.IngestionEndpoint, docs); err != nil {
		w.logger.Error("failed to send to pipeline", zap.Error(err))
		return err
	}

	// Delete old resource findings
	err = es.DeleteOtherResourceFindingsExcept(w.logger, w.esClient, resourceIds, j.ID)
	if err != nil {
		return err
	}

	w.logger.Info("Finished summarizer",
		zap.Uint("job_id", j.ID),
		zap.String("benchmark_id", j.BenchmarkID),
		zap.Int("resource_count", len(jd.ResourcesFindings)),
	)
	return nil
}
