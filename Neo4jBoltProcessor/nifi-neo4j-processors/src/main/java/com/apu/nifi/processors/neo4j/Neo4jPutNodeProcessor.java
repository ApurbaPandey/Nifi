package com.apu.nifi.processors.neo4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.shell.util.json.JSONObject;

public class Neo4jPutNodeProcessor extends Neo4jBoltAbstractProcessor{
	
	private Driver driver;
	private Session session;
	
	private List<String> props;
	private String cypherQuery;
	
	public static final PropertyDescriptor NEO4J_NODE = new PropertyDescriptor.Builder()
            .name("neo4j-node")
            .displayName("Neo4j Node")
            .description("Neo4j Node name (Case Sensitive)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor NEO4J_NODE_PROPS = new PropertyDescriptor.Builder()
            .name("neo4j-node-properties")
            .displayName("Neo4j Node Prpperties")
            .description("List of comma seperated properties which will be added to the node from the Attribute.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	@Override
    public Set<Relationship> getRelationships() {
		final Set<Relationship> relationships = new HashSet<Relationship>();
		
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		
		return relationships;
	}
	
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        
        descriptors.add(NEO4J_BOLT_URL);
        descriptors.add(NEO4J_USER);
        descriptors.add(NEO4J_PWD);
        descriptors.add(NEO4J_NODE);
        descriptors.add(NEO4J_NODE_PROPS);
        
        return descriptors;
	}

	@Override
	@OnScheduled
	void createNeo4jSession(ProcessContext context) {
		String url = context.getProperty(NEO4J_BOLT_URL).getValue();
		String user = context.getProperty(NEO4J_USER).getValue();
		String pwd = context.getProperty(NEO4J_PWD).getValue();
		
		AuthToken token = AuthTokens.basic(user, pwd);
		
		driver = GraphDatabase.driver(url, token);
		session = driver.session();
		
		String nodeName = context.getProperty(NEO4J_NODE).getValue();
		
		props = Arrays.asList(context.getProperty(NEO4J_NODE_PROPS).getValue().split(Neo4jProcessorConstants.COMMA));
		buildCypherQuery(nodeName);
	}

	private void buildCypherQuery(String nodeName) {

		try {
			JSONObject obj = new JSONObject();
			
			for(String prop : props){
				obj.put(prop, Neo4jProcessorConstants.OPEN_PARANTH+prop+Neo4jProcessorConstants.CLOSE_PARANTH);
			}
			
			cypherQuery = Neo4jProcessorConstants.CREATE_QUERY
					.replace(Neo4jProcessorConstants.QUERY_NODE_NAME, nodeName)
					.replace(Neo4jProcessorConstants.QUERY_PROPS_JSON, obj.toString())
					.replace(Neo4jProcessorConstants.QUOTES, Neo4jProcessorConstants.SPACE);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	@Override
	@OnStopped
	@OnRemoved
	@OnUnscheduled
	void closeNeo4jSession() {
		session.close();
		driver.close();
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		
		
		
	}
	
	

}
