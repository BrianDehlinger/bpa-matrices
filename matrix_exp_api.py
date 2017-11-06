#!/usr/bin/env python
from argparse import ArgumentParser
import datetime
import shutil
from utils import graphql_api

main_header_order = [
    'organization',
    'project',
    'submitter_id',
    'study_design',
    'study_objective',
    'library_preparation_kit_name', 
    'barcoding_applied', 
    'instrument_model', 
    'is_paired_end', 
    'read_length', 
    'target_capture_kit_name'
]

header_strings = {
    'organization': 'Organization',
    'project': 'Project Name',
    'submitter_id': 'Experiment ID',
    'study_design': 'Study Design',
    'study_objective': 'Study Objective',
    'library_preparation_kit_name': 'Library Preparation Method', 
    'barcoding_applied': 'Applied Barcoding?', 
    'instrument_model': 'Sequencer Type', 
    'is_paired_end': 'Library Layout', 
    'read_length': 'Read Lengths', 
    'target_capture_kit_name': 'Target Capture Method'
}

multiple_fields = {
    'read_length': ['read_length_lower', 'read_length_upper']
}

special_style = {
    'project': 'min-width:200px',
    'submitter_id': 'min-width:200px',
    'study_objective': 'min-width:200px',
    'study_design': 'min-width:200px',
    'type_of_specimen': 'min-width:200px',    
    'read_length_lower': 'min-width:100px',
    'library_preparation_kit_name': 'min-width:400px',
    'instrument_model': 'min-width:300px',
    'target_capture_kit_name': 'min-width:300px',
    'is_paired_end': 'min-width:100px',
    'read_length': 'min-width:100px'
}

not_validated_projects = ["internal-test"]

step = 5

def parse_cmd_args():
    ''' Read arguments '''

    parser = ArgumentParser()
    parser.add_argument('--copy_file_to_server',
                        help='copies file to object store',
                        action='store_true')
    parser.add_argument('--keys_file',
                        help='File for api authorization',
                        default='/home/ubuntu/.secrets')
    parser.add_argument('--output',
                        help='HTML output file',
                        default='matrix3.html')

    parser.set_defaults(print_list=False)
    args = parser.parse_args()
    
    return args


def read_group_query(project_id, experiment_id):
   ''' Get Read Group properties through GraphQL queries '''

   data_dict = {'library_preparation_kit_name': [], 
                'barcoding_applied': [], 
                'instrument_model': [], 
                'is_paired_end': [], 
                'read_length': [], 
                'target_capture_kit_name': []
   }

   query_txt = """query Cases {study (first:0, project_id: "%s", submitter_id: "%s") {   
                                    cases(first:0){submitter_id _biospecimens_count}}}"""% (project_id, experiment_id)

   data = graphql_api.query(query_txt, auth) 

   row_keys = []
   for case in data['data']['study'][0]['cases']:

      counts = case['_biospecimens_count']
      offset = 0

      case_data = {}
      while offset <= counts:

          query_txt = """query ReadGroups {case (project_id: "%s", submitter_id: "%s") {   
                                           biospecimens(first:%s, offset:%s){
                                           samples(first:0){
                                           aliquots(first:0){
                                           analytes(first:0){
                                           read_groups(first:0){
                                           library_preparation_kit_name barcoding_applied instrument_model is_paired_end read_length_lower read_length_upper target_capture_kit_name}}}}}}}"""  % (project_id, case['submitter_id'], step, offset)

          case_output = graphql_api.query(query_txt, auth) 

          if not case_data:
             case_data = case_output
          else:
             case_data['data']['case'][0]['biospecimens'] = case_data['data']['case'][0]['biospecimens'] + case_output['data']['case'][0]['biospecimens']
          offset += step

      for c in case_data['data']['case']:
          for b in c['biospecimens']: 
            for s in b['samples']:            
               for a in s['aliquots']:
                  for an in a['analytes']:
                    for rg in an['read_groups']:
                       if rg:
                          row = {}
                          row_key = ""
                          for key in data_dict:
                              if key in multiple_fields:
                                 value = ""
                                 if rg[multiple_fields[key][0]] != None:
                                     value = str(rg[multiple_fields[key][0]])
                                 if rg[multiple_fields[key][1]] != None and rg[multiple_fields[key][1]] != rg[multiple_fields[key][0]]:
                                     value = value + '-' + str(rg[multiple_fields[key][1]])
                                 row[key] = value
                                 row_key += value
                              elif rg[key] != None:
                                 row[key] = str(rg[key])
                                 row_key += str(rg[key])

                          if row_key not in row_keys:
                               row_keys.append(row_key)
                               for key in row:
                                   data_dict[key].append(row[key])

   paired_ends =[]
   for entry in data_dict['is_paired_end']:
      if entry == 'False':
          paired_ends.append('Single-End')
      else:
          paired_ends.append('Paired-End')         
   
   data_dict['is_paired_end'] = paired_ends
   
   return data_dict


def study_description(project_id, experiment_id, auth):
   ''' Get study properties through GraphQL queries '''

   query_txt = """query Study {study (first:0, project_id: "%s", submitter_id: "%s") {   
                                   submitter_id study_design study_objective}}"""  % (project_id, experiment_id)
   data = graphql_api.query(query_txt, auth) 

   return data['data']['study'][0]


def output_results_table(data, org_counts, proj_counts, file_name):
    ''' Format results in the output html '''

    print '***Writing HTML out***'
    with open(file_name, 'w') as out_file:
        out_file.write('<html>\n')
        out_file.write('<head>\n')
        out_file.write('<link rel="stylesheet" type="text/css" href="style.css">\n')
        out_file.write('<title>BloodPAC - Genetic Analysis Summary</title>\n')
        out_file.write('<link rel="shortcut icon" href="favicon.png" type="image/x-icon"/>\n')
        out_file.write('</head>\n')
        out_file.write('<body>\n')
        out_file.write('<div class="master-content">\n')
        out_file.write('<div class="container">\n')
        out_file.write('<div class="grid">\n')
        out_file.write('<section class="grid__col--60 grid__col--push10" role="header" id="header">\n')
        out_file.write('<h1><img src="bpa-logo.png" height="64"></h1>\n')
        out_file.write('<h2>Genetic Analysis Summary</h2>')
        out_file.write('</section>\n')
        out_file.write('<section class="grid__col--60 grid__col--push10" role="main" id="table">\n')
        out_file.write('<table style = "width:100%">\n')
        out_file.write('<thead>\n')
        for key in main_header_order:  
           if key in special_style:
              out_file.write('<th style="%s">%s</th>' % (special_style[key], header_strings[key]))
           else:
              out_file.write('<th>%s</th>' % header_strings[key])
        out_file.write('</thead>\n')       
        added_orgs = []
        added_proj = []
        for line in data:
           out_file.write('<tr>\n') 
           for key in main_header_order:  
              if key == 'organization':
                  if not line[key] in added_orgs:
                    out_file.write('<th class="organization" rowspan="%s">%s</th>' % (org_counts[line[key]], line[key]))
                    added_orgs.append(line[key])
              elif key == 'project':
                  if not line[key] in added_proj:
                    out_file.write('<td rowspan="%s">%s</td>' % (proj_counts[line[key]], line[key]))
                    added_proj.append(line[key])                
              else:
                  if not line[key]:
                     line[key] = '--'
                  if isinstance(line[key], list) and not None in line[key]: 
                     line[key] = '<br/>'.join(line[key])  
                  out_file.write('<td>%s</td>' % line[key])
           out_file.write('</tr>\n') 

        out_file.write('</table></section></div></div></div>\n')
        out_file.write('Last processed: %s UTC\n' % datetime.datetime.today().isoformat())
        out_file.write('</body></html>\n')


if __name__ == '__main__':

    args = parse_cmd_args()

    auth = graphql_api.get_api_auth(args.keys_file)  
    projects = graphql_api.get_projects(auth, not_validated_projects)

    data = []
    counts_by_org = {}
    counts_by_proj = {}
    for project in projects:

       print project
       org_name = project.replace('-','_').split('_')[1] 
       proj_name = project.replace('bpa-', '')
       if project in not_validated_projects: continue

       # Get and count studies per organization
       exp_counts, experiments = graphql_api.count_experiments(project, auth, 'Genetic Analysis', 'read_group')
       if org_name in counts_by_org:
          counts_by_org[org_name]+=exp_counts
       else:   
          counts_by_org[org_name]=exp_counts

       # Get study and read group data for each project
       for exp in experiments:
          data_exp = study_description(project, exp, auth)
          data_exp['organization'] = org_name
          data_exp['project'] = proj_name
          if proj_name in counts_by_proj:
             counts_by_proj[proj_name] += 1
          else:
             counts_by_proj[proj_name] = 1        

          data_rg = read_group_query(project, exp)
          
          # Combine study and read_group data
          data_exp.update(data_rg)          
          data.append(data_exp)

    # Prepare output HTML
    file_name = args.output
    nginx_loc = '/usr/share/nginx/html/'
    output_results_table(data, counts_by_org, counts_by_proj, args.output)

    # Copy results to server
    if args.copy_file_to_server:
        print "Copying %s to %s" % (file_name,
            nginx_loc + file_name)
        shutil.copyfile(file_name, nginx_loc + file_name)
