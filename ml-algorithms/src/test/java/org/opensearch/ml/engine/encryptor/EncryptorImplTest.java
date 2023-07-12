package org.opensearch.ml.engine.encryptor;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.ml.common.CommonValue.CREATE_TIME_FIELD;
import static org.opensearch.ml.common.CommonValue.MASTER_KEY;
import static org.opensearch.ml.common.CommonValue.ML_CONFIG_INDEX;

public class EncryptorImplTest {
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();
    @Mock
    Client client;

    @Mock
    ClusterService clusterService;

    @Mock
    ClusterState clusterState;

    String masterKey;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        masterKey = "0000000000000001";

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            GetResponse response = mock(GetResponse.class);
            when(response.isExists()).thenReturn(true);
            when(response.getSourceAsMap())
                    .thenReturn(ImmutableMap.of(MASTER_KEY, masterKey, CREATE_TIME_FIELD, Instant.now().toEpochMilli()));
            listener.onResponse(response);
            return null;
        }).when(client).get(any(), any());


        when(clusterService.state()).thenReturn(clusterState);

        Metadata metadata = new Metadata.Builder()
                .indices(ImmutableMap
                        .<String, IndexMetadata>builder()
                        .put(ML_CONFIG_INDEX, IndexMetadata.builder(ML_CONFIG_INDEX)
                                .settings(Settings.builder()
                                        .put("index.number_of_shards", 1)
                                        .put("index.number_of_replicas", 1)
                                        .put("index.version.created", Version.CURRENT.id))
                                .build())
                        .build()).build();
        when(clusterState.metadata()).thenReturn(metadata);
    }

    @Test
    public void encrypt() {
        Encryptor encryptor = new EncryptorImpl(clusterService, client);
        Assert.assertNull(encryptor.getMasterKey());
        String encrypted = encryptor.encrypt("test");
        Assert.assertNotNull(encrypted);
        Assert.assertEquals(masterKey, encryptor.getMasterKey());
    }

    @Test
    public void decrypt() {
        Encryptor encryptor = new EncryptorImpl(clusterService, client);
        Assert.assertNull(encryptor.getMasterKey());
        String encrypted = encryptor.encrypt("test");
        String decrypted = encryptor.decrypt(encrypted);
        Assert.assertEquals("test", decrypted);
        Assert.assertEquals(masterKey, encryptor.getMasterKey());
    }

    @Test
    public void encrypt_NullMasterKey_NullMasterKey_MasterKeyNotExistInIndex() {
        exceptionRule.expect(ResourceNotFoundException.class);
        exceptionRule.expectMessage("ML encryption master key not initialized yet");

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            GetResponse response = mock(GetResponse.class);
            when(response.isExists()).thenReturn(false);
            listener.onResponse(response);
            return null;
        }).when(client).get(any(), any());

        Encryptor encryptor = new EncryptorImpl(clusterService, client);
        Assert.assertNull(encryptor.getMasterKey());
        encryptor.encrypt("test");
    }

    @Test
    public void decrypt_NullMasterKey_GetMasterKey_Exception() {
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("test error");

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("test error"));
            return null;
        }).when(client).get(any(), any());

        Encryptor encryptor = new EncryptorImpl(clusterService, client);
        Assert.assertNull(encryptor.getMasterKey());
        encryptor.decrypt("test");
    }

    @Test
    public void decrypt_MLConfigIndexNotFound() {
        exceptionRule.expect(ResourceNotFoundException.class);
        exceptionRule.expectMessage("ML encryption master key not initialized yet");

        Metadata metadata = new Metadata.Builder().indices(ImmutableMap.of()).build();
        when(clusterState.metadata()).thenReturn(metadata);

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("test error"));
            return null;
        }).when(client).get(any(), any());

        Encryptor encryptor = new EncryptorImpl(clusterService, client);
        Assert.assertNull(encryptor.getMasterKey());
        encryptor.decrypt("test");
    }
}