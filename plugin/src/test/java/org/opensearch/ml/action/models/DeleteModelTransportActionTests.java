/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.action.models;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ml.action.models.DeleteModelTransportAction.BULK_FAILURE_MSG;
import static org.opensearch.ml.action.models.DeleteModelTransportAction.OS_STATUS_EXCEPTION_MESSAGE;
import static org.opensearch.ml.action.models.DeleteModelTransportAction.SEARCH_FAILURE_MSG;
import static org.opensearch.ml.action.models.DeleteModelTransportAction.TIMEOUT_MSG;
import static org.opensearch.ml.common.CommonValue.ML_MODEL_INDEX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.ScrollableHitSource;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.model.MLModelState;
import org.opensearch.ml.common.transport.model.MLModelDeleteRequest;
import org.opensearch.ml.helper.ModelAccessControlHelper;
import org.opensearch.ml.model.MLModelManager;
import org.opensearch.ml.utils.TestHelper;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class DeleteModelTransportActionTests extends OpenSearchTestCase {
    @Mock
    ThreadPool threadPool;

    @Mock
    Client client;

    @Mock
    TransportService transportService;

    @Mock
    ActionFilters actionFilters;

    @Mock
    ActionListener<DeleteResponse> actionListener;

    @Mock
    DeleteResponse deleteResponse;

    @Mock
    BulkByScrollResponse bulkByScrollResponse;

    @Mock
    NamedXContentRegistry xContentRegistry;

    @Mock
    private MLModelManager mlModelManager;

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Mock
    ClusterService clusterService;

    DeleteModelTransportAction deleteModelTransportAction;
    MLModelDeleteRequest mlModelDeleteRequest;
    ThreadContext threadContext;
    MLModel model;

    @Mock
    private ModelAccessControlHelper modelAccessControlHelper;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);

        mlModelDeleteRequest = MLModelDeleteRequest.builder().modelId("test_id").build();

        Settings settings = Settings.builder().build();
        deleteModelTransportAction = spy(
            new DeleteModelTransportAction(
                transportService,
                actionFilters,
                client,
                xContentRegistry,
                clusterService,
                modelAccessControlHelper
            )
        );

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(3);
            listener.onResponse(true);
            return null;
        }).when(modelAccessControlHelper).validateModelGroupAccess(any(), any(), any(), any());

        threadContext = new ThreadContext(settings);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
    }

    public void testDeleteModel_Success() throws IOException {
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(1);
            listener.onResponse(deleteResponse);
            return null;
        }).when(client).delete(any(), any());

        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            BulkByScrollResponse response = new BulkByScrollResponse(new ArrayList<>(), null);
            listener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        GetResponse getResponse = prepareMLModel(MLModelState.REGISTERED, null);
        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(), any());

        deleteModelTransportAction.doExecute(null, mlModelDeleteRequest, actionListener);
        verify(actionListener).onResponse(deleteResponse);
    }

    public void testDeleteModel_Success_AlgorithmNotNull() throws IOException {
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(1);
            listener.onResponse(deleteResponse);
            return null;
        }).when(client).delete(any(), any());

        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            BulkByScrollResponse response = new BulkByScrollResponse(new ArrayList<>(), null);
            listener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        GetResponse getResponse = prepareMLModel(MLModelState.REGISTERED, null);
        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(), any());

        deleteModelTransportAction.doExecute(null, mlModelDeleteRequest, actionListener);
        verify(actionListener).onResponse(deleteResponse);
    }

    public void test_Success_ModelGroupIDNotNull_LastModelOfGroup() throws IOException {
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(1);
            listener.onResponse(deleteResponse);
            return null;
        }).when(client).delete(any(), any());

        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            BulkByScrollResponse response = new BulkByScrollResponse(new ArrayList<>(), null);
            listener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        SearchResponse searchResponse = createModelGroupSearchResponse(1);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), isA(ActionListener.class));

        GetResponse getResponse = prepareMLModel(MLModelState.REGISTERED, "modelGroupID");

        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(), any());

        deleteModelTransportAction.doExecute(null, mlModelDeleteRequest, actionListener);
        verify(actionListener).onResponse(deleteResponse);
    }

    public void test_Success_ModelGroupIDNotNull_NotLastModelOfGroup() throws IOException {
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(1);
            listener.onResponse(deleteResponse);
            return null;
        }).when(client).delete(any(), any());

        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            BulkByScrollResponse response = new BulkByScrollResponse(new ArrayList<>(), null);
            listener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        SearchResponse searchResponse = createModelGroupSearchResponse(2);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), isA(ActionListener.class));

        MLModel mlModel = MLModel
            .builder()
            .modelId("test_id")
            .modelGroupId("modelGroupID")
            .modelState(MLModelState.REGISTERED)
            .algorithm(FunctionName.TEXT_EMBEDDING)
            .build();
        XContentBuilder content = mlModel.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);
        BytesReference bytesReference = BytesReference.bytes(content);
        GetResult getResult = new GetResult("indexName", "111", 111l, 111l, 111l, true, bytesReference, null, null);
        GetResponse getResponse = new GetResponse(getResult);
        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(), any());

        deleteModelTransportAction.doExecute(null, mlModelDeleteRequest, actionListener);
        verify(actionListener).onResponse(deleteResponse);
    }

    public void test_Failure_FailedToSearchLastModel() throws IOException {
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(1);
            listener.onResponse(deleteResponse);
            return null;
        }).when(client).delete(any(), any());

        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            BulkByScrollResponse response = new BulkByScrollResponse(new ArrayList<>(), null);
            listener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onFailure(new Exception("Failed to search Model index"));
            return null;
        }).when(client).search(any(), isA(ActionListener.class));

        GetResponse getResponse = prepareMLModel(MLModelState.REGISTERED, "modelGroupID");

        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(), any());

        deleteModelTransportAction.doExecute(null, mlModelDeleteRequest, actionListener);
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(actionListener).onFailure(argumentCaptor.capture());
        assertEquals("Failed to search Model index", argumentCaptor.getValue().getMessage());
    }

    public void test_UserHasNoAccessException() throws IOException {
        GetResponse getResponse = prepareMLModel(MLModelState.REGISTERED, "modelGroupID");
        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(3);
            listener.onResponse(false);
            return null;
        }).when(modelAccessControlHelper).validateModelGroupAccess(any(), any(), any(), any());

        deleteModelTransportAction.doExecute(null, mlModelDeleteRequest, actionListener);
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(actionListener).onFailure(argumentCaptor.capture());
        assertEquals("User doesn't have privilege to perform this operation on this model", argumentCaptor.getValue().getMessage());
    }

    public void testDeleteModel_CheckModelState() throws IOException {
        GetResponse getResponse = prepareMLModel(MLModelState.DEPLOYING, null);
        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(), any());

        deleteModelTransportAction.doExecute(null, mlModelDeleteRequest, actionListener);
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(actionListener).onFailure(argumentCaptor.capture());
        assertEquals(
            "Model cannot be deleted in deploying or deployed state. Try undeploy model first then delete",
            argumentCaptor.getValue().getMessage()
        );
    }

    public void testDeleteModel_ModelNotFoundException() throws IOException {
        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new Exception("Fail to find model"));
            return null;
        }).when(client).get(any(), any());

        deleteModelTransportAction.doExecute(null, mlModelDeleteRequest, actionListener);
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(actionListener).onFailure(argumentCaptor.capture());
        assertEquals("Fail to find model", argumentCaptor.getValue().getMessage());
    }

    public void testDeleteModel_ResourceNotFoundException() throws IOException {
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(1);
            listener.onFailure(new ResourceNotFoundException("errorMessage"));
            return null;
        }).when(client).delete(any(), any());

        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            BulkByScrollResponse response = new BulkByScrollResponse(new ArrayList<>(), null);
            listener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        GetResponse getResponse = prepareMLModel(MLModelState.REGISTERED, null);
        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(), any());

        deleteModelTransportAction.doExecute(null, mlModelDeleteRequest, actionListener);
        ArgumentCaptor<ResourceNotFoundException> argumentCaptor = ArgumentCaptor.forClass(ResourceNotFoundException.class);
        verify(actionListener).onFailure(argumentCaptor.capture());
        assertEquals("errorMessage", argumentCaptor.getValue().getMessage());
    }

    public void test_ValidationFailedException() throws IOException {
        GetResponse getResponse = prepareMLModel(MLModelState.REGISTERED, null);
        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(3);
            listener.onFailure(new Exception("Failed to validate access"));
            return null;
        }).when(modelAccessControlHelper).validateModelGroupAccess(any(), any(), any(), any());

        deleteModelTransportAction.doExecute(null, mlModelDeleteRequest, actionListener);
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(actionListener).onFailure(argumentCaptor.capture());
        assertEquals("Failed to validate access", argumentCaptor.getValue().getMessage());
    }

    public void testModelNotFound() throws IOException {
        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(null);
            return null;
        }).when(client).get(any(), any());
        deleteModelTransportAction.doExecute(null, mlModelDeleteRequest, actionListener);
        ArgumentCaptor<OpenSearchStatusException> argumentCaptor = ArgumentCaptor.forClass(OpenSearchStatusException.class);
        verify(actionListener).onFailure(argumentCaptor.capture());
        assertEquals("Failed to find model", argumentCaptor.getValue().getMessage());
    }

    public void testDeleteModelChunks_Success() {
        when(bulkByScrollResponse.getBulkFailures()).thenReturn(null);
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            listener.onResponse(bulkByScrollResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        deleteModelTransportAction.deleteModelChunks("test_id", deleteResponse, actionListener);
        verify(actionListener).onResponse(deleteResponse);
    }

    public void testDeleteModel_RuntimeException() throws IOException {
        GetResponse getResponse = prepareMLModel(MLModelState.REGISTERED, null);
        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(), any());

        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("errorMessage"));
            return null;
        }).when(client).delete(any(), any());

        deleteModelTransportAction.doExecute(null, mlModelDeleteRequest, actionListener);
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(actionListener).onFailure(argumentCaptor.capture());
        assertEquals("errorMessage", argumentCaptor.getValue().getMessage());
    }

    @Ignore
    public void testDeleteModel_ThreadContextError() {
        when(threadPool.getThreadContext()).thenThrow(new RuntimeException("thread context error"));
        deleteModelTransportAction.doExecute(null, mlModelDeleteRequest, actionListener);
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(actionListener).onFailure(argumentCaptor.capture());
        assertEquals("thread context error", argumentCaptor.getValue().getMessage());
    }

    public void test_FailToDeleteModel() {
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException("errorMessage"));
            return null;
        }).when(client).execute(any(), any(), any());

        deleteModelTransportAction.deleteModelChunks("test_id", deleteResponse, actionListener);
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(actionListener).onFailure(argumentCaptor.capture());
        assertEquals("errorMessage", argumentCaptor.getValue().getMessage());
    }

    public void test_FailToDeleteAllModelChunks() {
        BulkItemResponse.Failure failure = new BulkItemResponse.Failure(ML_MODEL_INDEX, "test_id", new RuntimeException("Error!"));
        when(bulkByScrollResponse.getBulkFailures()).thenReturn(Arrays.asList(failure));
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            listener.onResponse(bulkByScrollResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        deleteModelTransportAction.deleteModelChunks("test_id", deleteResponse, actionListener);
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(actionListener).onFailure(argumentCaptor.capture());
        assertEquals(OS_STATUS_EXCEPTION_MESSAGE + ", " + BULK_FAILURE_MSG + "test_id", argumentCaptor.getValue().getMessage());
    }

    public void test_FailToDeleteAllModelChunks_TimeOut() {
        BulkItemResponse.Failure failure = new BulkItemResponse.Failure(ML_MODEL_INDEX, "test_id", new RuntimeException("Error!"));
        when(bulkByScrollResponse.getBulkFailures()).thenReturn(Arrays.asList(failure));
        when(bulkByScrollResponse.isTimedOut()).thenReturn(true);
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            listener.onResponse(bulkByScrollResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        deleteModelTransportAction.deleteModelChunks("test_id", deleteResponse, actionListener);
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(actionListener).onFailure(argumentCaptor.capture());
        assertEquals(OS_STATUS_EXCEPTION_MESSAGE + ", " + TIMEOUT_MSG + "test_id", argumentCaptor.getValue().getMessage());
    }

    public void test_FailToDeleteAllModelChunks_SearchFailure() {
        ScrollableHitSource.SearchFailure searchFailure = new ScrollableHitSource.SearchFailure(
            new RuntimeException("error"),
            ML_MODEL_INDEX,
            123,
            "node_id"
        );
        when(bulkByScrollResponse.getBulkFailures()).thenReturn(new ArrayList<>());
        when(bulkByScrollResponse.isTimedOut()).thenReturn(false);
        when(bulkByScrollResponse.getSearchFailures()).thenReturn(Arrays.asList(searchFailure));
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            listener.onResponse(bulkByScrollResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        deleteModelTransportAction.deleteModelChunks("test_id", deleteResponse, actionListener);
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(actionListener).onFailure(argumentCaptor.capture());
        assertEquals(OS_STATUS_EXCEPTION_MESSAGE + ", " + SEARCH_FAILURE_MSG + "test_id", argumentCaptor.getValue().getMessage());
    }

    public GetResponse prepareMLModel(MLModelState mlModelState, String modelGroupID) throws IOException {
        MLModel mlModel = MLModel.builder().modelId("test_id").modelState(mlModelState).modelGroupId(modelGroupID).build();
        XContentBuilder content = mlModel.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);
        BytesReference bytesReference = BytesReference.bytes(content);
        GetResult getResult = new GetResult("indexName", "111", 111l, 111l, 111l, true, bytesReference, null, null);
        GetResponse getResponse = new GetResponse(getResult);
        return getResponse;
    }

    private SearchResponse createModelGroupSearchResponse(long totalHits) throws IOException {
        SearchResponse searchResponse = mock(SearchResponse.class);
        String modelContent = "{\n"
            + "                    \"created_time\": 1684981986069,\n"
            + "                    \"access\": \"public\",\n"
            + "                    \"latest_version\": 0,\n"
            + "                    \"last_updated_time\": 1684981986069,\n"
            + "                    \"name\": \"model_group_IT\",\n"
            + "                    \"description\": \"This is an example description\"\n"
            + "                }";
        SearchHit modelGroup = SearchHit.fromXContent(TestHelper.parser(modelContent));
        SearchHits hits = new SearchHits(new SearchHit[] { modelGroup }, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), Float.NaN);
        when(searchResponse.getHits()).thenReturn(hits);
        return searchResponse;
    }
}
