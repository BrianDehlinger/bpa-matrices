#!/usr/bin/env python
from utils import graphql_api
from argparse import ArgumentParser
import shutil
import datetime

mtde_fields = {
    "blood_tube_type": "biospecimen",
    "shipping_temperature": "biospecimen",    
    "composition": "sample",
    "hours_to_fractionation": "sample",    
    "clinical_or_contrived": "aliquot",    
    "hours_to_freezer": "aliquot",
    "storage_temperature": "aliquot",
    "analyte_isolation_method": "analyte",
    "assay_method": "quantification_assay"  
    #"assay_method": "assay_result"
    # DNA Yield    
}

multiple_fields = {
    "hours_to_fractionation": ["hours_to_fractionation_lower", "hours_to_fractionation_upper"],     
    "hours_to_freezer": ["hours_to_freezer_lower", "hours_to_freezer_upper"]
}


mtde_headers = {
    "blood_tube_type": "Tube Type",
    "shipping_temperature": "Shipping Temperature (Range)",    
    "composition": "Composition",
    "hours_to_fractionation": "Time to Fractionation (Range)",    
    "clinical_or_contrived": "Sample Type",    
    "hours_to_freezer": "Time to Freezer (Range)",
    "storage_temperature": "Storage Temperature",
    "analyte_isolation_method": "Analyte Isolation Method",
    "assay_method": "Quantification Method"  
    #"assay_method": "Assay Methodology"
}

not_validated_projects = ["internal-test"]

def parse_cmd_args():
    parser = ArgumentParser()
    parser.add_argument('--copy_file_to_server',
                        help='copies file to object store',
                        action='store_true')
    parser.add_argument('--keys_file',
                        help='File for api authorization',
                        default='/home/ubuntu/.secrets')

    parser.set_defaults(print_list=False)
    args = parser.parse_args()
    
    return args


def query_mtde_field(node, f, projects, auth):
    ''' Query summary counts for each data type '''
    
    if f in multiple_fields:
       field = " ".join(multiple_fields[f])
    else:
       field = f

    summary = {}
    for project in projects:
        
        query_txt = """query { %s(first:0, project_id: "%s") {%s}} """ % (node, project, field) 
        
        data = graphql_api.query(query_txt, auth)

        for d in data['data'][node]:
          
            if f in multiple_fields:
              if d[multiple_fields[f][0]] == d[multiple_fields[f][1]]:
                  field_name = str(d[multiple_fields[f][0]])
              else:
                  field_name = str(d[multiple_fields[f][0]]) + '-' + str(d[multiple_fields[f][1]])
            elif isinstance(d[field], basestring):
               field_name = d[field].encode('utf-8')
            else:
               field_name = str(d[field])
            
            if field_name not in summary:
                summary[field_name] = {} 
            
            summary[field_name].setdefault(project, 0)                 
            summary[field_name][project] += 1
    
    #plot_summary(summary, field)
    
    return summary


def output_matrix_table(summaries, projects, file_name):
    print '***Writing HTML out***'
    totals = {}
    with open(file_name, 'w') as out_file:
        out_file.write('<html>\n')
        out_file.write('<head>\n')
        out_file.write('<link rel="stylesheet" type="text/css" href="style.css">\n')
        out_file.write('<title>BloodPAC - Validated Data Summary</title>\n')
        out_file.write('<link rel="shortcut icon" href="favicon.png" type="image/x-icon"/>\n')
        out_file.write('</head>\n')
        out_file.write('<body>\n')
        out_file.write('<div class="master-content">\n')
        out_file.write('<div class="container">\n')
        out_file.write('<div class="grid">\n')
        out_file.write('<section class="grid__col--60 grid__col--push10" role="header" id="header">\n')
        out_file.write('<h1><img src="bpa-logo.png" height="64"></h1>\n')
        out_file.write('<h1>Minimum Technical Data Elements (MTDE)</h1>')  
        out_file.write('<h2>Data Summary Matrices</h2>')               
        out_file.write('</section>\n')

        for su in mtde_headers:
            totals = {}
            data = summaries[su]
            out_file.write('<button class="accordion">%s</button>' % mtde_headers[su])
            out_file.write('<div class="panel">')
            out_file.write('<section class="grid__col--60 grid__col--push10" role="main" id="table">\n')
            out_file.write('<table style = "width:100%">\n')
            out_file.write('<thead>\n')
            out_file.write('<tr>\n')               
            out_file.write('<th>Organization</th>')
            out_file.write('<th>Project</th>')
            for key in data:
                out_file.write('<th>%s</th>' % str(key))
                totals[key] = 0
            out_file.write('</tr>\n') 
            out_file.write('</thead>\n')

            for p in projects:
                proj_name = p.replace('bpa-', '')
                out_file.write('<tr>\n')
                out_file.write('<th class="organization">%s</th>' % proj_name.split('_')[0])
                out_file.write('<td>%s</td>' % proj_name)                
                for key in data:
                    if data[key] and p in data[key]:
                        out_file.write('<td style="min-width:150px">%s</td>' % data[key][p])
                        totals[key] += int(data[key][p])
                    else:
                        out_file.write('<td>--</td>')                 
                out_file.write('</tr>\n')

            out_file.write('<tfoot>\n')
            out_file.write('<th class="organization">TOTALS</th>')
            out_file.write('<td>%s</td>' % len(projects))
            for key in data:            
                out_file.write('<td>%s</td>' % totals[key])
            out_file.write('</tfoot>\n')



            out_file.write('</table>\n')
            out_file.write('Last processed: %s UTC\n' % datetime.datetime.today().isoformat())
            out_file.write('</section></div>')
        
        out_file.write('</div></div></div>')
        out_file.write('</body></html>\n')
        out_file.write('<script src="accordion.js"></script>')


if __name__ == '__main__':

  args = parse_cmd_args()

  auth = graphql_api.get_api_auth(args.keys_file) 

  projects = graphql_api.get_projects(auth, not_validated_projects)

  matrix_file_name = 'matrix_mtde.html'     
  nginx_loc = '/usr/share/nginx/html/'

  data = {}
  for mtde in mtde_fields:   
     print "Getting %s summary counts..." % (mtde)
     summary = query_mtde_field(mtde_fields[mtde], mtde, projects, auth)
     data[mtde] = summary

  output_matrix_table(data, projects, matrix_file_name)
  file_name = matrix_file_name

  if args.copy_file_to_server:
     print "Copying %s to %s" % (file_name,
         nginx_loc + file_name)
     shutil.copyfile(file_name, nginx_loc + file_name)
