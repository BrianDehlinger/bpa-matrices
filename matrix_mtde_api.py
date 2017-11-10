#!/usr/bin/env python
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.style as style
from matplotlib.lines import Line2D
import matplotlib.patches as mpatches
style.use('ggplot')
from utils import graphql_api
from argparse import ArgumentParser
import shutil
import datetime
import glob
import cgi

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
    "hours_to_freezer": ["hours_to_freezer_lower", "hours_to_freezer_upper"],
    "shipping_temperature": ["shipping_temperature", "shipping_temperature"],
    "storage_temperature": ["storage_temperature", "storage_temperature"]
}

mtde_headers = {
    "blood_tube_type": "Tube Type",
    "shipping_temperature": "Shipping Temperature",    
    "composition": "Composition",
    "hours_to_fractionation": "Time to Fractionation",    
    "clinical_or_contrived": "Sample Type",    
    "hours_to_freezer": "Time to Freezer",
    "storage_temperature": "Storage Temperature",
    "analyte_isolation_method": "Analyte Isolation Method",
    "assay_method": "Quantification Method"  
    #"assay_method": "Assay Methodology"
}

step = 10000

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

        count_query = """{ _%s_count(project_id: "%s") }""" % (node, project)
        counts = graphql_api.query(count_query, auth)['data']['_%s_count' % (node)]
        offset = 0  
      
        data = {}
        while offset <= counts:
           query_txt = """{ %s(first:%s, offset:%s, project_id: "%s") {%s}}""" % (node, step, offset, project, field)
           output = graphql_api.query(query_txt, auth)
           if not data:
              data = output
           else:
              data['data'][node] = data['data'][node] + output['data'][node]
           offset += step

        for d in data['data'][node]:
          
            if f in multiple_fields:
              if d[multiple_fields[f][0]] == d[multiple_fields[f][1]]:
                  field_name = str(d[multiple_fields[f][0]])
              else:
                  field_name = str(d[multiple_fields[f][0]]) + '-' + str(d[multiple_fields[f][1]])
            elif isinstance(d[field], basestring):
                field_name = d[field]
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
        out_file.write('<title>BloodPAC - MTDE Summary</title>\n')
        out_file.write('<link rel="shortcut icon" href="favicon.png" type="image/x-icon"/>\n')
        out_file.write('</head>\n')
        out_file.write('<body>\n')
        out_file.write('<div class="master-content">\n')
        out_file.write('<div class="container">\n')
        out_file.write('<div class="grid">\n')
        out_file.write('<section class="grid__col--60 grid__col--push10" role="header" id="header">\n')
        out_file.write('<h1><img src="bpa-logo.png" height="64"></h1>\n')
        out_file.write('<h1>Minimum Technical Data Elements (MTDE)</h1>')  
        out_file.write('<h2>MTDE Summary Matrices</h2>')               
        out_file.write('</section>\n')

        for su in mtde_headers:
          if su in summaries:
            totals = {}
            data = summaries[su]

            out_file.write('<button class="accordion">%s</button>' % mtde_headers[su])
            out_file.write('<div class="panel">')
            out_file.write('<section class="grid__col--60 grid__col--push10" role="main" id="table">\n')

            linkElement = 'link_' + su
            divTable = 'table_' + su
            divPlot = 'plot_' + su
            if su in multiple_fields:
                imagename = plot_time_fields(data, su, projects)
            else:
                imagename = plot_fields(data, su, projects)                  

            out_file.write('<a id="%s" href="#%s" onclick="showPlot(\'%s\',\'%s\',\'%s\')", class="showplot">View as a graph</a>' % (linkElement, su, divTable, divPlot, linkElement))
            out_file.write('<div id="%s", style="display:none">' % divPlot)
            out_file.write('<img src="%s" alt="Fractination Time" style="width:100vw;">' % imagename)
            out_file.write('</div>')
         
            out_file.write('<div id="%s">' % divTable)
            out_file.write('<table style = "width:100%">\n')
            out_file.write('<thead>\n')
            out_file.write('<tr>\n')               
            out_file.write('<th>Organization</th>')
            out_file.write('<th>Project</th>')
            for key in data:
                out_file.write('<th>%s</th>' % key.encode('utf-8'))
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
            out_file.write('</div>')
            out_file.write('Last processed: %s UTC\n' % datetime.datetime.today().isoformat())
            out_file.write('</section></div>')
        
        out_file.write('</div></div></div>')
        out_file.write('</body></html>\n')
        out_file.write('<script src="accordion.js"></script>')
        out_file.write('<script src="showPlot.js"></script>')

def plot_time_fields(data, su, projects):

    ylabels = []
    bars = {}
    pos = 0
    positions = []
    maxi = 0
    mini = -2
    plt.figure(figsize=(32,15))
    not_applicable = []
    for p in projects:
        proj_name = p.replace('bpa-', '')
        ylabels.append(proj_name)            
        pos += 1
        positions.append(pos)
        for key in data:
            if data[key] and p in data[key]:
                if key == "None":
                    not_applicable.append(pos)
                    continue
                elif '-' in key[1:-1]:
                    limits = key.split("-")
                else:
                    limits = [float(key) - 0.5, float(key) + 0.5]

                length = float(limits[1]) - float(limits[0])
                plt.barh(pos, length, left=float(limits[0]), height=0.5, align='center', edgecolor='brown', color='#bd1f2f')

                if float(limits[1]) > maxi:
                   maxi = float(limits[1])

                if float(limits[0]) < mini:
		   mini = float(limits[0])

    for p in not_applicable:
        if mini < 0: 
            limits = [mini-1.5, mini-0.5]
        else:
            limits = [-1.5, -0.5]

        length = float(limits[1]) - float(limits[0])
        plt.barh(p, float(limits[1]) - float(limits[0]), left=float(limits[0]), height=0.5, align='center', edgecolor='brown', color='c')

    plt.ylim(float(positions[0]-1),float(positions[-1])+1)    
    plt.xlim(mini-2, maxi+5)
    if mini < 0:
       plt.xticks(range(0, int(mini)-1, -10) + range(10, int(maxi)+1, 10), fontsize = 18)
    else:
       plt.xticks(range(0, int(maxi)+1, 12), fontsize = 18)   

    locsy, labelsy = plt.yticks(positions,ylabels)
    plt.setp(labelsy, fontsize = 18)
    plt.xlabel(mtde_headers[su], fontsize=24)
    plt.legend(handles=[mpatches.Patch(color='c', label='Not Applicable/Unknown')], fontsize = 18, loc='best')
    img = 'mtde_%s.svg' % (su)
    plt.savefig(img, bbox_inches='tight') # svg
    plt.show()

    return img


def plot_fields(data, su, projects):

    ylabels = []
    xlabels = []
    xvalues = []
    yvalues = []
    xpos = 0
    ypos = 0    
    ypositions = []
    xpositions = []
    values = []
    colors = []
    circles = []
    plt.figure(figsize=(50, 30))
    for p in projects:
        proj_name = p.replace('bpa-', '')
        ypos += 1
        ylabels.append(proj_name)
        ypositions.append(ypos)
        xpos = 0
        for key in data:
            xpos += 1
            pr_color = plt.get_cmap("tab20")(xpos-1)
            xlab = key.replace(' - ', '\n').replace('. ', '\n')
            if xlab not in xlabels:
                xlabels.append(xlab)
                xpositions.append(xpos)
                circles.append(Line2D([0], [0], marker="o", markersize=24, color='white', markerfacecolor=pr_color, alpha=0.5))
            if data[key] and p in data[key]:
                xvalues.append(xpos)
                yvalues.append(ypos)
                values.append(data[key][p])                 
                plt.text(xpos, ypos, str(data[key][p]), fontsize=24, horizontalalignment='center')
                colors.append(pr_color)

    step = 10000/max(values) + 1
    area = [1000 + v*step for v in values]
    plt.legend(circles, xlabels, fontsize = 24, loc='upper right')
    plt.scatter(xvalues, yvalues, s=area, linewidths=2, edgecolor='w', alpha=0.5, color=colors)
    plt.xticks(xpositions, '', fontsize = 24)
    plt.yticks(ypositions, ylabels, fontsize = 24)
    plt.ylim(0, len(ylabels) + 1)
    plt.xlim(0.5, len(xlabels) * 1.2)
    plt.xlabel(mtde_headers[su], fontsize = 24)
    img = 'mtde_%s.svg' % (su)
    plt.savefig(img, bbox_inches='tight')   
    plt.show()

    return img


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
     print "Copying %s to %s" % (file_name, nginx_loc + file_name)
     shutil.copyfile(file_name, nginx_loc + file_name)
     for file in glob.glob('*.svg'):
         shutil.copy(file, nginx_loc)
