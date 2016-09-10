package com.apu.nifi.processors.neo4j;

import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

public class Neo4jPutNodeProcessor extends Neo4jBoltAbstractProcessor{
	
	private Driver driver;
	private Session session;
	

	@Override
	@OnScheduled
	void createNeo4jSession(ProcessContext context) {
		String url = context.getProperty(NEO4J_BOLT_URL).getValue();
		String user = context.getProperty(NEO4J_USER).getValue();
		String pwd = context.getProperty(NEO4J_PWD).getValue();
		
		AuthToken token = AuthTokens.basic(user, pwd);
		
		driver = GraphDatabase.driver(url, token);
		session = driver.session();
		
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
	public void onTrigger(ProcessContext arg0, ProcessSession arg1) throws ProcessException {
		// TODO Auto-generated method stub
		
	}

}
