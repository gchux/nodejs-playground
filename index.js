const express = require('express')
const {grpc, CallOptions} = require('google-gax');
const {Spanner} = require('@google-cloud/spanner');

const app = express()
const port = 8080
const projectID = process.env.PROJECT_ID;

// Creates a client
const spanner = new Spanner({
  projectId: projectID,
  apiEndpoint: 'grpc.local',
  port: 5001,
  sslCreds: grpc.credentials.createInsecure()
});

// Gets a reference to a Cloud Spanner instance and database
const instanceAdminClient = spanner.getInstanceAdminClient();

async function listInstances(request, options) {
  response = await instanceAdminClient.listInstances(request, options)
  return response
}

app.get('/spanner/listInstances/:project_id', async (req, res) => {
  const request = {
    parent: `projects/${req.params['project_id']}`
  }
  response = await listInstances(request, {
    otherArgs: {
      headers: {
        'x-goog-user-project': [projectID],
        'x-grpc-proxy-project': [projectID],
        'x-grpc-proxy-endpoint': ["spanner.googleapis.com:443"],
        'x-cloud-trace-context': req.headers['x-cloud-trace-context'],
      }
    }
  })
  res.set({
    'x-cloud-trace-context': req.headers['x-cloud-trace-context'],
  })
  res.send(response)
})

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})
