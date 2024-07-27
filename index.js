const express = require('express')
const {grpc, CallOptions} = require('google-gax');
const {Spanner} = require('@google-cloud/spanner');
const _ = require('lodash');
const extend = require('extend');

const app = express()
const port = 8080

const X_CLOUD_TRACE_CONTEXT = "x-cloud-trace-context";
const X_GOOG_USER_PROJECT = "x-goog-user-project";
const X_GRPC_PROXY_PROJECT = "x-grpc-proxy-project"
const X_GRPC_PROXY_ENDPOINT = "x-grpc-proxy-endpoint"

const PROJECT_ID = process.env.PROJECT_ID;
const SPANNER_API_ENDPOINT = "spanner.googleapis.com:443"
const SPANNER_INSTANCE_ID = process.env.SPANNER_INSTANCE_ID;
const SPANNER_DATABASE_ID = process.env.SPANNER_DATABASE_ID;

var spanner = null
var instance = null
var database = null

const clientOtherArgs = {
  otherArgs: {
    headers: {
      'x-goog-user-project': [PROJECT_ID],
      'x-grpc-proxy-project': [PROJECT_ID],
      'x-grpc-proxy-endpoint': [SPANNER_API_ENDPOINT],
    }
  }
}

const sendResponse = (
  httpRequest,
  httpResponse,
  data,
) => {
  httpResponse.set({
    [X_CLOUD_TRACE_CONTEXT]: httpRequest.headers[X_CLOUD_TRACE_CONTEXT],
  })
  httpResponse.send(data)
} 

const getGaxOptions = (
  httpRequest,
  projectID = PROJECT_ID,
) => {
  return _.merge({}, clientOtherArgs, {
    otherArgs: {
      headers: {
        [X_GOOG_USER_PROJECT]: [projectID],
        [X_GRPC_PROXY_PROJECT]: [projectID],
        [X_GRPC_PROXY_ENDPOINT]: [SPANNER_API_ENDPOINT],
        [X_CLOUD_TRACE_CONTEXT]: httpRequest.headers[X_CLOUD_TRACE_CONTEXT],
      }
    }
  });
}

const _listInstances = async (
  request,
  options,
) => {
  const instanceAdminClient = spanner?.getInstanceAdminClient();
  response = await instanceAdminClient.listInstances(request, options)
  return response
}

const _isDbReady = async (
  httpRequest,
  projectID = PROJECT_ID,
) => {

  if (spanner == null || database == null) {
    console.error('_isDbReady failed, Spaner DB not ready');
    return false;
  }

  gaxOptions = _.merge({},
    getGaxOptions(httpRequest, projectID),
    {
      timeout: 1500,
    }
  );

  const query = {
    sql: 'SELECT 1',
    gaxOptions: gaxOptions,
  }

  try {
    const [rows] = await database.run(query);
    if(rows?.length) {
      return true;
    }
    console.log('_isDbReady - DB test returned no rows')
  } catch(e) {
    console.error(`_isDbReady exception cought: `, e.toString());
    console.error(e.stack)
  }

  return false
}

const _initDb = async (
  httpRequest,
  projectID = PROJECT_ID,
  instanceID = SPANNER_INSTANCE_ID,
  databaseID = SPANNER_DATABASE_ID
) => {
  try {
    spanner?.close();
    spanner = new Spanner({
      projectId: projectID,
      apiEndpoint: 'grpc.local',
      port: 5001,
      sslCreds: grpc.credentials.createInsecure(),
    });

    spanner.grpcMetadata.add(X_GOOG_USER_PROJECT, projectID);
    spanner.grpcMetadata.add(X_GRPC_PROXY_PROJECT, projectID);
    spanner.grpcMetadata.add(X_GRPC_PROXY_ENDPOINT, SPANNER_API_ENDPOINT);

    instance = spanner.instance(instanceID);
    database = instance.database(databaseID, {
      acquireTimeout: 3000
    });

    // sessions are are created by `SessionPool`:
    //   - `batchCreateSessions` does accept `gaxOptions`:
    //     - see: https://github.com/googleapis/nodejs-spanner/blob/v7.10.0/src/database.ts#L644-L690
    //   but `SessionsPool` does not care about providing any kind of flexibility to configure its RPC
    //     - see: https://github.com/googleapis/nodejs-spanner/blob/v7.10.0/src/session-pool.ts#L740-L744
    database.resourceHeader_[X_GOOG_USER_PROJECT] = projectID;
    database.resourceHeader_[X_GRPC_PROXY_PROJECT] = projectID;
    database.resourceHeader_[X_GRPC_PROXY_ENDPOINT] = SPANNER_API_ENDPOINT;
    database.resourceHeader_[X_CLOUD_TRACE_CONTEXT] = httpRequest.headers[X_CLOUD_TRACE_CONTEXT];
    
    if(await _isDbReady(httpRequest, projectID)) {
      console.log('Spanner DB is ready now!');
      return true;
    }
  } catch(e){
    console.error( `_initDb exception cought: `, e.toString());
    console.error(e.stack)
  }

  console.error('Failed to Initialize Spaner DB.');
  return false;
}

app.get('/init/:project_id/:instance_id/:database_id', 
  async (httpRequest, httpResponse) => {

    const isDbReady = await _initDb(
      httpRequest,
      httpRequest.params['project_id'] || PROJECT_ID,
      httpRequest.params['instance_id'] || SPANNER_INSTANCE_ID,
      httpRequest.params['database_id'] || SPANNER_DATABASE_ID,
    )

    sendResponse(httpRequest, httpResponse, isDbReady)
  })

app.get('/test/:project_id', 
  async (httpRequest, httpResponse) => {

    isDbReady = await _isDbReady(
      httpRequest,
      httpRequest.params['project_id'] || PROJECT_ID,
    )

    sendResponse(httpRequest, httpResponse, isDbReady)
  })

app.get('/listInstances/:project_id',
  async (httpRequest, httpResponse) => {
    const projectID = req.params['project_id']
    const request = {
      parent: `projects/${projectID}`
    }
    const response = await _listInstances(request, getGaxOptions(httpRequest, projectID));
    sendResponse(httpRequest, httpResponse, response)
  })

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})
