#!/usr/bin/env python
from argparse import ArgumentParser
import datetime
import shutil
from utils import graphql_api

main_header_order = [
    'organization',
    'project',
    'submitter_id',
    'assay_category',
    'assay_instrument',
    'assay_instrument_model',
    'assay_method'
]

header_strings = {
    'organization': 'Organization',
    'project': 'Project Name',
    'submitter_id': 'Experiment ID',
    'assay_category': ' Category',
    'assay_instrument': 'Assay Instrument',
    'assay_instrument_model': 'Instrument Model',
    'assay_method': 'Assay Methodology'
}

assays = {
    'mass_cytometry_assay': 'Mass Cytometry',
    'quantification_assay': 'Quantification',
    'pcr_assay': 'PCR',
    'immunoassay': 'Immunoassay'
}

special_style = {
    'project': 'min-width:200px',
    'submitter_id': 'min-width:200px',
    'assay_instrument': 'min-width:300px', 
    'assay_instrument_model': 'min-width:300px', 
    'assay_method': 'min-width:300px',         
}

not_validated_projects = ["internal-test", "bpa-USC_OPT1_T1"]

def parse_cmd_args():
    ''' Read arguments '''

    parser = ArgumentParser()
    parser.add_argument('--copy_file_to_server',
                        help='copies file to object store',
                        action='store_true')
    parser.add_argument('--keys_file',
                        help='File for api authorization',
                        default='/home/ubuntu/credentials.json')
    parser.add_argument('--output',
                        help='HTML output file',
                        default='matrix_assays.html')

    parser.set_defaults(print_list=False)
    args = parser.parse_args()
    
    return args


def assay_queries(project_id, experiment_id):
   ''' Get assay properties through GraphQL queries '''

   data_dict = {
            'assay_category': [],
            'assay_instrument': [],
            'assay_instrument_model': [],
            'assay_method': []                                 
   }

   for assay in assays:

     assay_pl = assay + 's'

     query_txt = """query Cases {study (first:0, project_id: "%s", submitter_id: "%s", with_path_to:{type: "%s"}) {   
                                      cases(first:0){submitter_id}}}"""% (project_id, experiment_id, assay)

     data = graphql_api.query(query_txt, auth) 

     if data['data']['study']:
       data_dict['assay_category'].append(assays[assay])
       row_keys = []
       for case in data['data']['study'][0]['cases']:
          query_txt = """query ReadGroups {case (first:0, project_id: "%s", submitter_id: "%s") {   
                                          biospecimens(first:0){
                                          samples(first:0){
                                          aliquots(first:0){
                                          analytes(first:0){
                                          %s(first:0){
                                              assay_instrument
                                              assay_instrument_model
                                              assay_method
                                          }}}}}}}"""  % (project_id, case['submitter_id'], assay_pl)

          case_data = graphql_api.query(query_txt, auth) 

          for c in case_data['data']['case']:
              for b in c['biospecimens']: 
                for s in b['samples']:            
                   for al in s['aliquots']:
                      for an in al['analytes']:
                        for a in an[assay_pl]:
                           if a:
                              row = {}
                              row_key = ""
                              for key in data_dict:
                                if key != 'assay_category' and a[key] != None and not str(a) in data_dict[key]:  
                                    row[key] = str(a[key])
                                    row_key += str(a[key])

                              if row_key not in row_keys:
                                row_keys.append(row_key)
                                for key in row:
                                    data_dict[key].append(row[key])

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
        out_file.write('<title>BloodPAC - Assays Summary</title>\n')
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
       exp_counts, experiments = graphql_api.count_experiments(project, auth)
       if org_name in counts_by_org:
          counts_by_org[org_name]+=exp_counts
       else:   
          counts_by_org[org_name]=exp_counts

       # Get study and assay data for each project
       for exp in experiments:
          data_exp = study_description(project, exp, auth)
          data_exp['organization'] = org_name
          data_exp['project'] = proj_name
          if proj_name in counts_by_proj:
             counts_by_proj[proj_name] += 1
          else:
             counts_by_proj[proj_name] = 1        
          
          data_assay = assay_queries(project, exp)

          # Combine study and assays data
          if data_assay['assay_category']:
             data_exp.update(data_assay)          
             data.append(data_exp)
          else:
             counts_by_org[org_name] -= 1
             counts_by_proj[proj_name] -= 1

    # Prepare output HTML
    file_name = args.output
    nginx_loc = '/usr/share/nginx/html/'
    output_results_table(data, counts_by_org, counts_by_proj, args.output)

    # Copy results to server
    if args.copy_file_to_server:
        print "Copying %s to %s" % (file_name,
            nginx_loc + file_name)
        shutil.copyfile(file_name, nginx_loc + file_name)
