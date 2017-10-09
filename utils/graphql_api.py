from cdispyutils.hmac4 import get_auth
import requests
import time
import json

def get_api_auth(filename):
  
  with open(filename,'r') as f:
      secrets = json.load(f)
  auth = get_auth(secrets['access_key'], secrets['secret_key'], 'submission')
  
  return auth


def query(query_txt, auth, variables=None):

   if variables == None:
      query = {'query': query_txt}
   else:
      query = {'query': query_txt, 'variables': variables}    

   #print query_txt
   tries = 0
   while tries < 1:
      output = requests.post('http://kubenode.internal.io:30004/v0/submission/graphql/', auth=auth, json=query).text
      data = json.loads(output)  

      if 'errors' in data:
         print data

      if not 'data' in data:
         print query_txt
         print data
      
      else:
         if not data['data']:
            tries += 1
            time.sleep(1)
 	    auth = get_api_auth('/home/ubuntu/.secrets')
	 else:
	    break            

   return data

def get_projects(auth, excluded=[]):
   
   query_txt = """query Project { project(first:0) {project_id}} """
   
   data = query(query_txt, auth) 
   
   projects = []
   for pr in data['data']['project']:
      if pr['project_id'] not in excluded:
      	 projects.append(pr['project_id'])
   projects = sorted(projects)   

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
