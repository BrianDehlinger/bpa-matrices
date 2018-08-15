from cdispyutils.hmac4 import get_auth
import requests
import time
import json

def get_api_auth(filename):
  
  json_data=open(filename).read()
  keys = json.loads(json_data)
  auth = requests.post('https://data.bloodpac.org/user/credentials/cdis/access_token', json=keys)

  return auth


def query(query_txt, auth, variables=None):

   if variables == None:
      query = {'query': query_txt}
   else:
      query = {'query': query_txt, 'variables': variables}    

   output = requests.post('https://data.bloodpac.org/api/v0/submission/graphql/', headers={'Authorization': 'bearer '+ auth.json()['access_token']}, json=query).text
   data = json.loads(output)  

   if 'errors' in data:
      print data['errors']

   if not 'data' in data:
      print query_txt
      print data
      
   return data

def get_projects(auth, excluded=[]):
   
   query_txt = """query Project { project(first:0) {project_id}} """
   
   data = query(query_txt, auth) 
   
   projects = []
   for pr in data['data']['project']:
      if pr['project_id'] not in excluded:
      	 projects.append(pr['project_id'])
   projects = sorted(projects)   

   print projects

   return projects

def count_experiments(project_id, auth, study_setup=None, path="read_group"):

   if study_setup == None:
      query_txt = """query Project {study(first:0, project_id:"%s") {   
                                    submitter_id}}""" % (project_id)
   else:
      query_txt = """query Project {study(first:0, project_id:"%s", study_setup: "%s", with_path_to:{type: "%s"}) {     
                                submitter_id}}""" % (project_id, study_setup, path)

   data = query(query_txt, auth) 
   counts = 0
   experiments = []

   for study in data['data']['study']:
      counts += 1
      experiments.append(study['submitter_id'])
   
   return counts, experiments   
