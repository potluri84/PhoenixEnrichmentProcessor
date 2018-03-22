package com.sam.enrichment.processor;

import com.hortonworks.streamline.streams.Result;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl.Builder;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoenixEnrichmentProcessor
  implements CustomProcessorRuntime
{
  protected static final Logger LOG = LoggerFactory.getLogger(PhoenixEnrichmentProcessor.class);
  static final String CONFIG_ZK_SERVER_URL = "zkServerUrl";
  static final String CONFIG_ENRICHMENT_SQL = "enrichmentSQL";
  static final String CONFIG_ENRICHED_OUTPUT_FIELDS = "enrichedOutputFields";
  static final String CONFIG_SECURE_CLUSTER = "secureCluster";
  static final String CONFIG_KERBEROS_CLIENT_PRINCIPAL = "kerberosClientPrincipal";
  static final String CONFIG_KERBEROS_KEYTAB_FILE = "kerberosKeyTabFile";
  private Connection phoenixConnection = null;
  private String enrichmentSQLStatement = null;
  private String[] enrichedOutPutFields;
  private boolean secureCluster;
  
  public void cleanup()
  {
    DbUtils.closeQuietly(this.phoenixConnection);
  }
  
  public void initialize(Map<String, Object> config)
  {
    LOG.info("Initializing + " + PhoenixEnrichmentProcessor.class.getName());
    
    this.enrichmentSQLStatement = ((String)config.get("enrichmentSQL")).trim();
    LOG.info("The configured enrichment SQL is: " + this.enrichmentSQLStatement);
    
    String outputFields = (String)config.get("enrichedOutputFields");
    String outputFieldsCleaned = StringUtils.deleteWhitespace(outputFields);
    this.enrichedOutPutFields = outputFieldsCleaned.split(",");
    LOG.info("Enriched Output fields is: " + this.enrichedOutPutFields);
    
    LOG.info("Seure Cluster Flag is: " + config.get("secureCluster"));
    LOG.info("Kerberos Principal is: " + config.get("kerberosClientPrincipal"));
    LOG.info("Kerberos KeyTab is: " + config.get("kerberosKeyTabFile"));
    
    setUpJDBCPhoenixConnection(config);
  }
  
  public List<StreamlineEvent> process(StreamlineEvent event)
    throws ProcessingException
  {
    LOG.info("Event[" + event + "] about to be enriched");
    
    StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
    builder.putAll(event);
    
    Map<String, Object> enrichValues = enrich(event);
    LOG.info("Enriching events[" + event + "]  with the following enriched values: " + enrichValues);
    
    builder.putAll(enrichValues);
    
    List<Result> results = new ArrayList();
    
    StreamlineEvent enrichedEvent = builder.dataSourceId(event.getDataSourceId()).build();
    LOG.info("Enriched StreamLine Event is: " + enrichedEvent);
    
    List<StreamlineEvent> newEvents = Collections.singletonList(enrichedEvent);
    
    return newEvents;
  }
  
  private Map<String, Object> enrich(StreamlineEvent event)
  {
    Map<String, Object> enrichedValues = new HashMap();
    
    StrSubstitutor strSub = new StrSubstitutor(event);
    
    String enrichSQLToExecute = strSub.replace(this.enrichmentSQLStatement);
    ResultSet rst = null;
    Statement statement = null;
    try
    {
      LOG.info("The SQL with substitued fields to be executed is: " + enrichSQLToExecute);
      
      statement = this.phoenixConnection.createStatement();
      rst = statement.executeQuery(enrichSQLToExecute);
      if (rst.next())
      {
        int columnCount = rst.getMetaData().getColumnCount();
        int i = 1;
        for (int count = 0; i <= columnCount; i++) {
          enrichedValues.put(this.enrichedOutPutFields[(count++)], rst
            .getString(i));
        }
      }
      else
      {
        String errorMsg = "No results found for enrichment query: " + enrichSQLToExecute;
        
        LOG.error(errorMsg);
        throw new RuntimeException(errorMsg);
      }
    }
    catch (SQLException e)
    {
      String errorMsg = "Error enriching event[" + event + "] with enrichment sql[" + this.enrichmentSQLStatement + "]";
      
      LOG.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
    finally
    {
      DbUtils.closeQuietly(rst);
      DbUtils.closeQuietly(statement);
    }
    return enrichedValues;
  }
  
  public void validateConfig(Map<String, Object> config)
  {
    boolean isSecureCluster = (config.get("secureCluster") != null) && (((Boolean)config.get("secureCluster")).booleanValue());
    if (isSecureCluster)
    {
      String principal = (String)config.get("kerberosClientPrincipal");
      String keyTab = (String)config.get("kerberosKeyTabFile");
      if ((StringUtils.isEmpty(principal)) || (StringUtils.isEmpty(keyTab))) {
        LOG.error("If Secure Cluster, Kerberos principal and key tabe must be provided");
      }
    }
  }
  
  private String constructInSecureJDBCPhoenixConnectionUrl(String zkServerUrl)
  {
    StringBuffer buffer = new StringBuffer();
    buffer.append("jdbc:phoenix:").append(zkServerUrl)
      .append(":/hbase-unsecure");
    return buffer.toString();
  }
  
  private String constructSecureJDBCPhoenixConnectionUrl(String zkServerUrl, String clientPrincipal, String keyTabFile)
  {
    StringBuffer buffer = new StringBuffer();
    buffer.append("jdbc:phoenix:").append(zkServerUrl)
      .append(":/hbase-secure")
      .append(":").append(clientPrincipal)
      .append(":").append(keyTabFile);
    return buffer.toString();
  }
  
  private void setUpJDBCPhoenixConnection(Map<String, Object> config)
  {
    String zkServerUrl = (String)config.get("zkServerUrl");
    
    boolean secureCluster = (config.get("secureCluster") != null) && (((Boolean)config.get("secureCluster")).booleanValue());
    
    String clientPrincipal = (String)config.get("kerberosClientPrincipal");
    String keyTabFile = (String)config.get("kerberosKeyTabFile");
    
    String jdbcPhoenixConnectionUrl = "";
    if (secureCluster) {
      jdbcPhoenixConnectionUrl = constructSecureJDBCPhoenixConnectionUrl(zkServerUrl, clientPrincipal, keyTabFile);
    } else {
      jdbcPhoenixConnectionUrl = constructInSecureJDBCPhoenixConnectionUrl(zkServerUrl);
    }
    LOG.info("Initializing Phoenix Connection with JDBC connection string[" + jdbcPhoenixConnectionUrl + "]");
    try
    {
      this.phoenixConnection = DriverManager.getConnection(jdbcPhoenixConnectionUrl);
    }
    catch (SQLException e)
    {
      String error = "Error creating Phoenix JDBC connection";
      LOG.error(error, e);
      throw new RuntimeException(error);
    }
    LOG.info("Successfully created Phoenix Connection with JDBC connection string[" + jdbcPhoenixConnectionUrl + "]");
  }
}
