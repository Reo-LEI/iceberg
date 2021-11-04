/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.nessie;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.Tasks;
import org.projectnessie.api.TreeApi;
import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.client.NessieClient;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Contents;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableDelete;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Nessie implementation of Iceberg Catalog.
 *
 * <p>
 * A note on namespaces: Nessie namespaces are implicit and do not need to be explicitly created or deleted.
 * The create and delete namespace methods are no-ops for the NessieCatalog. One can still list namespaces that have
 * objects stored in them to assist with namespace-centric catalog exploration.
 * </p>
 */
public class NessieCatalog extends BaseMetastoreCatalog implements AutoCloseable, SupportsNamespaces, Configurable {
  private static final Logger logger = LoggerFactory.getLogger(NessieCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");
  private NessieClient client;
  private String warehouseLocation;
  private Configuration config;
  private UpdateableReference reference;
  private String name;
  private FileIO fileIO;
  private Map<String, String> catalogOptions;

  public NessieCatalog() {
  }

  @Override
  public void initialize(String inputName, Map<String, String> options) {
    this.catalogOptions = ImmutableMap.copyOf(options);
    String fileIOImpl = options.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO = fileIOImpl == null ? new HadoopFileIO(config) : CatalogUtil.loadFileIO(fileIOImpl, options, config);
    this.name = inputName == null ? "nessie" : inputName;
    // remove nessie prefix
    final Function<String, String> removePrefix = x -> x.replace("nessie.", "");

    this.client = NessieClient.builder().fromConfig(x -> options.get(removePrefix.apply(x))).build();

    this.warehouseLocation = options.get(CatalogProperties.WAREHOUSE_LOCATION);
    if (warehouseLocation == null) {
      // Explicitly log a warning, otherwise the thrown exception can get list in the "silent-ish catch"
      // in o.a.i.spark.Spark3Util.catalogAndIdentifier(o.a.s.sql.SparkSession, List<String>,
      //     o.a.s.sql.connector.catalog.CatalogPlugin)
      // in the code block
      //    Pair<CatalogPlugin, Identifier> catalogIdentifier = SparkUtil.catalogAndIdentifier(nameParts,
      //        catalogName ->  {
      //          try {
      //            return catalogManager.catalog(catalogName);
      //          } catch (Exception e) {
      //            return null;
      //          }
      //        },
      //        Identifier::of,
      //        defaultCatalog,
      //        currentNamespace
      //    );
      logger.warn("Catalog creation for inputName={} and options {} failed, because parameter " +
          "'warehouse' is not set, Nessie can't store data.", inputName, options);
      throw new IllegalStateException("Parameter 'warehouse' not set, Nessie can't store data.");
    }
    final String requestedRef = options.get(removePrefix.apply(NessieConfigConstants.CONF_NESSIE_REF));
    this.reference = loadReference(requestedRef);
  }

  @Override
  public void close() {
    client.close();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    TableReference pti = TableReference.parse(tableIdentifier);
    UpdateableReference newReference = this.reference;
    if (pti.reference() != null) {
      newReference = loadReference(pti.reference());
    }
    return new NessieTableOperations(
        NessieUtil.toKey(pti.tableIdentifier()),
        newReference,
        client,
        fileIO,
        catalogOptions);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier table) {
    if (table.hasNamespace()) {
      return SLASH.join(warehouseLocation, table.namespace().toString(), table.name());
    }
    return SLASH.join(warehouseLocation, table.name());
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return tableStream(namespace).collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    reference.checkMutable();

    IcebergTable existingTable = table(identifier);
    if (existingTable == null) {
      return false;
    }

    Operations contents = ImmutableOperations.builder()
        .addOperations(ImmutableDelete.builder().key(NessieUtil.toKey(identifier)).build())
        .commitMeta(NessieUtil.buildCommitMetadata(String.format("delete table %s", identifier), catalogOptions))
        .build();

    // We try to drop the table. Simple retry after ref update.
    boolean threw = true;
    try {
      Tasks.foreach(contents)
          .retry(5)
          .stopRetryOn(NessieNotFoundException.class)
          .throwFailureWhenFinished()
          .onFailure((c, exception) -> refresh())
          .run(c -> {
            Branch branch = client.getTreeApi().commitMultipleOperations(reference.getAsBranch().getName(),
                reference.getHash(), c);
            reference.updateReference(branch);
          }, BaseNessieClientServerException.class);
      threw = false;
    } catch (NessieConflictException e) {
      logger.error("Cannot drop table: failed after retry (update ref and retry)", e);
    } catch (NessieNotFoundException e) {
      logger.error("Cannot drop table: ref is no longer valid.", e);
    } catch (BaseNessieClientServerException e) {
      logger.error("Cannot drop table: unknown error", e);
    }
    return !threw;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier toOriginal) {
    reference.checkMutable();

    TableIdentifier to = NessieUtil.removeCatalogName(toOriginal, name());

    IcebergTable existingFromTable = table(from);
    if (existingFromTable == null) {
      throw new NoSuchTableException("table %s doesn't exists", from.name());
    }
    IcebergTable existingToTable = table(to);
    if (existingToTable != null) {
      throw new AlreadyExistsException("table %s already exists", to.name());
    }

    Operations contents = ImmutableOperations.builder()
        .addOperations(
            ImmutablePut.builder().key(NessieUtil.toKey(to)).contents(existingFromTable).build(),
            ImmutableDelete.builder().key(NessieUtil.toKey(from)).build())
        .commitMeta(NessieUtil.buildCommitMetadata("iceberg rename table", catalogOptions))
        .build();

    try {
      Tasks.foreach(contents)
          .retry(5)
          .stopRetryOn(NessieNotFoundException.class)
          .throwFailureWhenFinished()
          .onFailure((c, exception) -> refresh())
          .run(c -> {
            Branch branch = client.getTreeApi().commitMultipleOperations(reference.getAsBranch().getName(),
                reference.getHash(), c);
            reference.updateReference(branch);
          }, BaseNessieClientServerException.class);
    } catch (NessieNotFoundException e) {
      // important note: the NotFoundException refers to the ref only. If a table was not found it would imply that the
      // another commit has deleted the table from underneath us. This would arise as a Conflict exception as opposed to
      // a not found exception. This is analogous to a merge conflict in git when a table has been changed by one user
      // and removed by another.
      throw new RuntimeException("Failed to drop table as ref is no longer valid.", e);
    } catch (BaseNessieClientServerException e) {
      throw new CommitFailedException(e, "Failed to rename table: the current reference is not up to date.");
    } catch (HttpClientException ex) {
      // Intentionally catch all nessie-client-exceptions here and not just the "timeout" variant
      // to catch all kinds of network errors (e.g. connection reset). Network code implementation
      // details and all kinds of network devices can induce unexpected behavior. So better be
      // safe than sorry.
      throw new CommitStateUnknownException(ex);
    }
    // Intentionally just "throw through" Nessie's HttpClientException here and do not "special case"
    // just the "timeout" variant to propagate all kinds of network errors (e.g. connection reset).
    // Network code implementation details and all kinds of network devices can induce unexpected
    // behavior. So better be safe than sorry.
  }

  /**
   * creating namespaces in nessie is implicit, therefore this is a no-op. Metadata is ignored.
   *
   * @param namespace a multi-part namespace
   * @param metadata  a string Map of properties for the given namespace
   */
  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return tableStream(namespace)
        .map(TableIdentifier::namespace)
        .filter(n -> !n.isEmpty())
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * namespace metadata is not supported in Nessie, thus we return an empty map.
   *
   * @param namespace a namespace. {@link Namespace}
   * @return an empty map
   */
  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    return ImmutableMap.of();
  }

  /**
   * Namespaces in Nessie are implicit and therefore cannot be dropped
   *
   * @param namespace The {@link Namespace} to drop.
   * @throws UnsupportedOperationException Namespaces in Nessie are implicit and thus cannot be dropped.
   */
  @Override
  public boolean dropNamespace(Namespace namespace) {
    throw new UnsupportedOperationException(
        "Cannot drop namespace '" + namespace + "': dropNamespace is not supported by the NessieCatalog");
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) {
    throw new UnsupportedOperationException(
        "Cannot set properties for namespace '" + namespace +
            "': setProperties is not supported by the NessieCatalog");
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) {
    throw new UnsupportedOperationException(
        "Cannot remove properties for namespace '" + namespace +
            "': removeProperties is not supported by the NessieCatalog");
  }

  @Override
  public void setConf(Configuration conf) {
    this.config = conf;
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  TreeApi getTreeApi() {
    return client.getTreeApi();
  }

  public void refresh() throws NessieNotFoundException {
    reference.refresh();
  }

  public String currentHash() {
    return reference.getHash();
  }

  String currentRefName() {
    return reference.getName();
  }

  private IcebergTable table(TableIdentifier tableIdentifier) {
    try {
      Contents table = client.getContentsApi()
          .getContents(NessieUtil.toKey(tableIdentifier), reference.getName(), reference.getHash());
      return table.unwrap(IcebergTable.class).orElse(null);
    } catch (NessieNotFoundException e) {
      return null;
    }
  }

  private UpdateableReference loadReference(String requestedRef) {
    try {
      Reference ref = requestedRef == null ? client.getTreeApi().getDefaultBranch()
          : client.getTreeApi().getReferenceByName(requestedRef);
      return new UpdateableReference(ref, client.getTreeApi());
    } catch (NessieNotFoundException ex) {
      if (requestedRef != null) {
        throw new IllegalArgumentException(String.format(
            "Nessie ref '%s' does not exist. This ref must exist before creating a NessieCatalog.",
            requestedRef), ex);
      }

      throw new IllegalArgumentException(String.format(
          "Nessie does not have an existing default branch." +
              "Either configure an alternative ref via %s or create the default branch on the server.",
          NessieConfigConstants.CONF_NESSIE_REF), ex);
    }
  }

  private Stream<TableIdentifier> tableStream(Namespace namespace) {
    try {
      return client.getTreeApi()
          .getEntries(reference.getName(), EntriesParams.builder().hashOnRef(reference.getHash()).build())
          .getEntries()
          .stream()
          .filter(NessieUtil.namespacePredicate(namespace))
          .map(NessieUtil::toIdentifier);
    } catch (NessieNotFoundException ex) {
      throw new NoSuchNamespaceException(ex, "Unable to list tables due to missing ref. %s", reference.getName());
    }
  }
}
