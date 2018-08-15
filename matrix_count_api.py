#!/usr/bin/env python
from argparse import ArgumentParser
import shutil
import datetime
from utils import graphql_api

main_header_order = [
    'organization',
    'project',
    'study',
    'protocol',
    'case',
    'clinical',
    'biospecimen',
    'sample',
    'aliquot',
    'analyte',
    'images',
    'contrived_expectation',
    'assays',
    'read_group',
    'sequencing_data',
    'submitted_copy_number',
    'submitted_methylation'
]

header_strings = {
    'organization': 'Organization',
    'project': 'Project',
    'study': 'Studies',
    'case': 'Cases',
    'biospecimen': 'Biospecimens',
    'clinical': 'Clinical Data',
    'demographic': 'Demographic Records',
    'family_history': 'Family History Records',
    'exposure': 'Exposure Records',
    'diagnosis': 'Diagnosis Records',
    'treatment': 'Treatment Records',
    'followup': 'Follow-Up Records',
    'sample': 'Samples',
    'contrived_expectation': 'Expected Mutations',
    'images': 'Images',
    'slide_image': 'Slide Images',
    'cell_image': 'Cell Images',
    'mass_cytometry_image': 'Mass Cytometry Image',
    'aliquot': 'Aliquots',
    'analyte': 'Analytes',
    'read_group': 'Read Groups',
    'assays': 'Assays',
    'immunoassay': 'Immunoassays',
    'pcr_assay': 'PCR',
    'mass_cytometry_assay': 'Mass Cytometry',
    'quantification_assay': 'Quantification',
    'sequencing_data': 'Sequencing Data',
    'sequencing_assay': 'Sequencing Assays',
    'submitted_unaligned_reads_file': 'Unaligned Reads Files', 
    'submitted_aligned_reads_file': 'Aligned Reads Files',    
    'submitted_somatic_mutations': 'Somatic Mutations Files', 
    'submitted_copy_number': 'Copy Number Files',
    'submitted_methylation': 'Methylation Files',
    'experimental_analysis': 'Experimental Analysis',
    'protocol': 'Protocol Documents'
}

non_total_columns = [
    'validated',
    'formats_not_supported'
]

multicolumns = {
    'clinical': ['demographic',
                 'family_history',
                 'exposure',
                 'diagnosis',
                 'treatment',
		             'followup'],

    'sequencing_data':  ['submitted_unaligned_reads_file',
                         'submitted_aligned_reads_file',
                         'submitted_somatic_mutations',
			 'sequencing_assay'],

    'assays': ['immunoassay',
              'pcr_assay',
              'mass_cytometry_assay',
              'quantification_assay'],

    'images': ['slide_image',
               'cell_image',
               'mass_cytometry_image']
}

count_fields = {
  'organization': 'organization',
  'project': 'project',
  'study': '_study_count',
  'protocol': '_protocol_count',  
  'case': '_case_count',
  'biospecimen': '_biospecimen_count',
  'demographic': '_demographic_count',
  'family_history': '_family_history_count',
  'diagnosis': '_diagnosis_count', 
  'exposure': '_exposure_count', 
  'treatment': '_treatment_count', 
  'followup': '_followup_count',        
  'sample': '_sample_count',
  'aliquot': '_aliquot_count',
  'analyte': '_analyte_count',
  'read_group': '_read_group_count',
  'submitted_somatic_mutations': '_submitted_somatic_mutation_count',
  'submitted_unaligned_reads_file': '_submitted_unaligned_reads_count',
  'submitted_aligned_reads_file': '_submitted_aligned_reads_count',
  'submitted_copy_number': '_submitted_copy_number_count', 
  'submitted_methylation': '_submitted_methylation_count',
  'contrived_expectation': '_contrived_expectation_count',
  'slide_count': '_slide_count_count',  
  'slide_image': '_slide_image_count',
  'cell_image': '_cell_image_count',
  'mass_cytometry_image': '_mass_cytometry_image_count',
  'pcr_assay': '_pcr_assay_count',
  'quantification_assay': '_quantification_assay_count',
  'mass_cytometry_assay': '_mass_cytometry_assay_count',
  'immunoassay': '_immunoassay_count',
  'sequencing_assay': '_sequencing_assay_count'
}

not_validated_projects = ["internal-test"]


def parse_cmd_args():
    parser = ArgumentParser()
    parser.add_argument('--copy_file_to_server',
                        help='copies file to object store',
                        action='store_true')
    parser.add_argument('--keys_file',
                        help='File for api authorization',
                        default='/home/ubuntu/credentials.json')

    parser.set_defaults(print_list=False)
    args = parser.parse_args()
    
    return args


def get_counts(project_id, auth):

   query_txt = """query Counts ($projectID: [String]) {
                      _case_count(project_id: $projectID)   
                      _biospecimen_count(project_id: $projectID)
                      _sample_count(project_id: $projectID)                        
                      _study_count(project_id: $projectID)
                      _protocol_count(project_id: $projectID)
                      _treatment_count(project_id: $projectID)
                      _demographic_count(project_id: $projectID)
                      _family_history_count(project_id: $projectID)
                      _exposure_count(project_id: $projectID)                                   
                      _diagnosis_count(project_id: $projectID)
                      _followup_count(project_id: $projectID)
                      _aliquot_count(project_id: $projectID)                                        
                      _analyte_count(project_id: $projectID)
                      _contrived_expectation_count(project_id: $projectID) 
                      _slide_image_count(project_id: $projectID)
                      _cell_image_count(project_id: $projectID)
                      _mass_cytometry_image_count(project_id: $projectID)                      
                      _mass_cytometry_assay_count(project_id: $projectID)
                      _pcr_assay_count(project_id: $projectID)
                      _immunoassay_count(project_id: $projectID)
                      _quantification_assay_count(project_id: $projectID)
                      _sequencing_assay_count(project_id: $projectID)
                      _read_group_count(project_id: $projectID)
                      _submitted_unaligned_reads_count(project_id: $projectID)
                      _submitted_aligned_reads_count(project_id: $projectID)
                      _submitted_somatic_mutation_count(project_id: $projectID)
                      _submitted_copy_number_count(project_id: $projectID)  
                      _submitted_methylation_count(project_id: $projectID)
               } """ 
   variables = { 'projectID': project_id }

   data = graphql_api.query(query_txt, auth, variables) 

   return data['data']


def output_matrix_table(data, file_name):
    print '***Writing HTML out***'
    totals = {}
    with open(file_name, 'w') as out_file:
        out_file.write('<html>\n')
        out_file.write('<head>\n')
        out_file.write('<link rel="stylesheet" type="text/css" href="style.css">\n')
        out_file.write('<title>BloodPAC - Data Summary</title>\n')
        out_file.write('<link rel="shortcut icon" href="favicon.png" type="image/x-icon"/>\n')
        out_file.write('</head>\n')
        out_file.write('<body>\n')
        out_file.write('<div class="master-content">\n')
        out_file.write('<div class="container">\n')
        out_file.write('<div class="grid">\n')
        out_file.write('<section class="grid__col--60 grid__col--push10" role="header" id="header">\n')
        out_file.write('<h1><img src="bpa-logo.png" height="64"></h1>\n')
        out_file.write('<h1>Data Summary Matrix</h1>')
        out_file.write('</section>\n')
        out_file.write('<section class="grid__col--60 grid__col--push10" role="main" id="table">\n')
        out_file.write('<table style = "width:100%">\n')
        out_file.write('<thead>\n')
        out_file.write('<tr>\n')        
        mc_order = []
        for key in main_header_order:  
           totals[key] = 0          
           if key in multicolumns:              
              out_file.write('<th colspan="%d">%s</th>' % (len(multicolumns[key]), header_strings[key]))
              mc_order.append(key)
           else:
              out_file.write('<th rowspan="2">%s</th>' % header_strings[key])
        out_file.write('</tr>\n') 
        out_file.write('<tr>\n')
        for mc in mc_order:
            for c in multicolumns[mc]:
              totals[c] = 0  
              out_file.write('<th>%s</th>' % header_strings[c])
        out_file.write('</tr>\n')
                               
        out_file.write('</thead>\n')       
        added_orgs = []
        added_proj = []
        for line in data:
           out_file.write('<tr>\n') 
           for key in main_header_order: 
                if key in multicolumns:
                   for subcolumn in multicolumns[key]:
                      if not line[count_fields[subcolumn]]:
                        line[count_fields[subcolumn]] = '--'
                      elif line[count_fields[subcolumn]] != '--':                         
                        totals[subcolumn] += int(line[count_fields[subcolumn]])     
                      out_file.write('<td>%s</td>' % line[count_fields[subcolumn]])   
                elif key == 'organization':
                    if not line[key] in added_orgs:
                      out_file.write('<th class="organization" >%s</th>' % (line[key]))
                else: 
                    count_key = count_fields[key]                  
                    if not line[count_key]:
                        line[count_key] = '--'
                    elif isinstance(line[count_key], list) and not None in line[count_key]: 
                        line[count_key] = ', '.join(line[count_key]) 
                        totals[key] = 'N/A'
                    elif key == 'project':
                        totals[key] += 1    
                    else:                         
                        totals[key] += int(line[count_key])     
                    out_file.write('<td>%s</td>' % line[count_key])
           out_file.write('</tr>\n')         
        out_file.write('<tfoot>\n')
        for key in main_header_order:            
              if key == 'organization':
                 out_file.write('<th class="organization" >TOTALS</th>')
              elif key in multicolumns:
                 for subcolumn in multicolumns[key]:
                    out_file.write('<td >%s</td>' % totals[subcolumn])                   
              else:
                 out_file.write('<td >%s</td>' % totals[key])
        out_file.write('</tfoot>\n')
        out_file.write('</table></section></div></div></div>\n')
        out_file.write('Last processed: %s UTC\n' % datetime.datetime.today().isoformat())
        out_file.write('</body></html>\n')

if __name__ == '__main__':

    args = parse_cmd_args()

    auth = graphql_api.get_api_auth(args.keys_file) 
    projects = graphql_api.get_projects(auth, not_validated_projects)

    data = []
    for project in projects:
       print project
       counts = {}
       org_name = project.replace('-','_').split('_')[1] 
       proj_name = project.replace('bpa-', '')
       counts = get_counts(project, auth)

       counts['organization'] = org_name
       counts['project']      = proj_name  

       data.append(counts)   

    matrix_file_name = 'matrix_api.html'
    nginx_loc = '/usr/share/nginx/html/'

    output_matrix_table(data, matrix_file_name)
    file_name = matrix_file_name

    if args.copy_file_to_server:
        print "Copying %s to %s" % (file_name,
            nginx_loc + file_name)
        shutil.copyfile(file_name, nginx_loc + file_name)
