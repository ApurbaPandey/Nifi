package com.apu.nifi.processors.neo4j;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;



public abstract class Neo4jBoltAbstractProcessor extends AbstractProcessor{
	
	public static final PropertyDescriptor NEO4J_BOLT_URL = new PropertyDescriptor.Builder()
            .name("neo4j-bolt-url")
            .displayName("Neo4j Bolt URL")
            .description("Neo4j Bolt URL which will be connected to, including host, port. The default is 7687.")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor NEO4J_USER = new PropertyDescriptor.Builder()
            .name("neo4j-user")
            .displayName("Neo4j User")
            .description("Neo4j User.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor NEO4J_PWD = new PropertyDescriptor.Builder()
            .name("neo4j-password")
            .displayName("Neo4j Password")
            .description("Neo4j Password.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are successfully added to Neo4j are routed to this relationship").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be failed to be added to Neo4j are routed to this relationship").build();
    
    abstract void createNeo4jSession(ProcessContext context);
    abstract void closeNeo4jSession();

}
