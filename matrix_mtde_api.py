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
import numpy as np
import pandas as pd
import seaborn as sns
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
    "assay_method": ["quantification_assay", "immunoassay", "mass_cytometry_assay", "pcr_assay", "sequencing_assay"],
    "molecular_concentration": "quantification_assay"
}

multiple_fields = {
    "hours_to_fractionation": ["hours_to_fractionation_upper", "hours_to_fractionation_upper"],     
    "hours_to_freezer": ["hours_to_freezer_upper", "hours_to_freezer_upper"],
    "shipping_temperature": ["shipping_temperature", "shipping_temperature"],
    "storage_temperature": ["storage_temperature", "storage_temperature"],
}

multiple_columns = {
    "assay_method": {
	"quantification_assay": "Quantification Assays",
        "immunoassay": "Immunoassays",
	"pcr_assay": "PCR Assays",
	"sequencing_assay": "Sequencing Assays",
	"mass_cytometry_assay": "Mass Cytometry Assays"
    }
}

mtde_headers = {
    "blood_tube_type": "Tube Type",
    "shipping_temperature": "Shipping Temperature",    
    "composition": "Composition",
    "hours_to_fractionation": "Maximum Time to Fractionation",    
    "clinical_or_contrived": "Sample Type",    
    "hours_to_freezer": "Maximum Time to Freezer",
    "storage_temperature": "Storage Temperature",
    "analyte_isolation_method": "Analyte Isolation Method",
    "quantification_assay": "Quantification Method",
    "assay_method": "Assay Method",
    "molecular_concentration": "DNA Concentration"
}

distributions = ["molecular_concentration"]

other_choices = ['None', 'Unknown', 'Not Applicable']

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

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

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
        errors = {}
        while offset <= counts:
           if f == "molecular_concentration":
               query_txt = """{ %s(first:%s, offset:%s, project_id: "%s", with_path_to: {type: "analyte", analyte_type: "DNA"}) {%s}}""" % (node, step, offset, project, field)
           else:
               query_txt = """{ %s(first:%s, offset:%s, project_id: "%s") {%s}}""" % (node, step, offset, project, field)
           output = graphql_api.query(query_txt, auth)
           if not data:
              data = output
           else:
              data['data'][node] = data['data'][node] + output['data'][node]
           offset += step

           if 'errors' in data:
               for e in other_choices:
                   err_count = 0

                   if f in multiple_fields and multiple_fields[f][0] != multiple_fields[f][1]:
   	              err_count = len([msg for msg in data['errors'] if e in msg])/len(multiple_fields[f])
                   else:
                      err_count = len([msg for msg in data['errors'] if e in msg])

                   if err_count > 0:
                      errors.setdefault(e, 0) 
                      errors[e] += err_count
                    
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
    
        for e in errors:
            summary.setdefault(e, {})
            summary[e].setdefault(project, 0)
            summary[e][project] = errors[e]
            if 'None' in summary and project in summary['None']:
                summary['None'][project] -= errors[e]
                if summary['None'][project] <= 0:
                    summary['None'].pop(project, None)
                    if not summary['None']:
                       summary.pop('None', None)

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
          if su in summaries or su in multiple_columns:
            totals = {}
            data = {}
            if su in multiple_columns:
                for assay in multiple_columns[su]:
                    data[assay] = summaries[assay]
            else:
                data[su] = summaries[su]

            out_file.write('<button class="accordion">%s</button>' % mtde_headers[su])
            out_file.write('<div class="panel">')
            out_file.write('<section class="grid__col--60 grid__col--push10" role="main" id="table">\n')

            linkElement = 'link_' + su
            divTable = 'table_' + su
            divPlot = 'plot_' + su
            tsvTable = ''
            if su in multiple_fields:
                imagename = plot_time_fields(data, su, projects)
            elif su in distributions:
                imagename = plot_distributions(data, su, projects) 
            else:
                imagename = plot_fields(data, su, projects)

            out_file.write('<a id="%s" href="#%s" onclick="showPlot(\'%s\',\'%s\',\'%s\')", class="showplot">View as a graph</a>' % (linkElement, su, divTable, divPlot, linkElement))
            out_file.write('<div id="%s", style="display:none">' % divPlot)
            out_file.write('<img src="%s" alt="%s" style="width:100vw;">' % (imagename, su))
            out_file.write('</div>')
         
            out_file.write('<div id="%s">' % divTable)
            out_file.write('<table style = "width:100%">\n')
            out_file.write('<thead>\n')

            if su in multiple_columns:  
    	        out_file.write('<tr>\n')
                out_file.write('<th rowspan="2">Organization</th>')
                out_file.write('<th rowspan="2">Project</th>' )
		for assay in multiple_columns[su]:
                    num_columns = len(data[assay].keys())
                    out_file.write('<th colspan="%d">%s</th>' % (num_columns, multiple_columns[su][assay]))
		out_file.write('</tr>\n')
            else: 
                out_file.write('<tr>\n')
                out_file.write('<th>Organization</th>')
                out_file.write('<th>Project</th>' )
            
            tsvTable += 'Organization\tProject'

            for a in data:
                sorted_keys = sorted([float(x) for x in data[a].keys() if is_number(x)]) + sorted([x for x in data[a].keys() if not is_number(x)])
                for key in sorted_keys:
                    if isinstance(key, float): key = str(key)
                    out_file.write('<th>%s</th>' % key.encode('utf-8'))
                    totals.setdefault(a, {})
                    totals[a].setdefault(key, 0)
                    if len(data) > 1:
                       table_header = a.encode('utf-8') + ' - ' +  key.encode('utf-8')
                    else:
                       table_header = key.encode('utf-8')
                    tsvTable += '\t%s' % table_header
            out_file.write('</tr>\n') 
            out_file.write('</thead>\n')
            tsvTable += '\n'

            for p in projects:
                proj_name = p.replace('bpa-', '')
                org_name = proj_name.split('_')[0]
                out_file.write('<tr>\n')
                out_file.write('<th class="organization">%s</th>' % org_name)
                out_file.write('<td>%s</td>' % proj_name)
                tsvTable += org_name.encode('utf-8') + '\t' + proj_name.encode('utf-8')
                for a in data:
                    sorted_keys = sorted([float(x) for x in data[a].keys() if is_number(x)]) + sorted([x for x in data[a].keys() if not is_number(x)])
                    for key in sorted_keys:
                       if isinstance(key, float): key = str(key)
                       if data[a][key] and p in data[a][key]:
                          out_file.write('<td style="min-width:150px">%s</td>' % data[a][key][p])
                          tsvTable += '\t%s' %  data[a][key][p]
                          totals[a][key] += data[a][key][p]
                       else:
                          out_file.write('<td>--</td>')
                          tsvTable += '\t0'         
                out_file.write('</tr>\n')
                tsvTable += '\n'

            out_file.write('<tfoot>\n')
            out_file.write('<th class="organization">TOTALS</th>')
            out_file.write('<td>%s</td>' % len(projects))

            for a in data:
               sorted_keys = sorted([float(x) for x in data[a].keys() if is_number(x)]) + sorted([x for x in data[a].keys() if not is_number(x)])
               for key in sorted_keys:
                   if isinstance(key, float): key = str(key)
                   out_file.write('<td>%s</td>' % totals[a][key])
            out_file.write('</tfoot>\n')

            out_file.write('</table>\n')
            out_file.write('</div>')
            table_file = divTable + '.tsv'
            out_file.write('<a href="%s">Download table to TSV file</a><br>' % table_file)
            out_file.write('Last processed: %s UTC\n' % datetime.datetime.today().isoformat())
            out_file.write('</section></div>')
        
            # Write table in TSV
            with open(table_file, 'w') as tf:
                 tf.write(tsvTable)

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
        for a in data:
          for key in data[a]:
            if data[a][key] and p in data[a][key]:
                if key in other_choices:
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
        for a in data:  
          for key in data[a]:
            xpos += 1
            pr_color = plt.get_cmap("tab20")(xpos-1)
            xlab = key.replace(' - ', '\n').replace('. ', '\n')
            if su in multiple_columns and a in multiple_columns[su]:
               xlab = multiple_columns[su][a] + ' - ' + xlab
            if xlab not in xlabels:
                xlabels.append(xlab)
                xpositions.append(xpos)
                circles.append(Line2D([0], [0], marker="o", markersize=24, color='white', markerfacecolor=pr_color, alpha=0.5))
            if data[a][key] and p in data[a][key]:
                xvalues.append(xpos)
                yvalues.append(ypos)
                values.append(data[a][key][p])                 
                plt.text(xpos, ypos, str(data[a][key][p]), fontsize=24, horizontalalignment='center')
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

def label(x, color, label):
    ax = plt.gca()
    ax.text(0, .2, label, fontweight="bold", fontsize=8, color=color, 
            ha="right", va="center", transform=ax.transAxes)

def plot_distributions(data, su, projects):

    names = []

    proj_names = []
    values = []
    for a in data:
        for key in data[a]:
            if key != "None":
               for p in data[a][key]:
                  proj_name = p.replace('bpa-', '')
                  proj_names += [proj_name] * data[a][key][p]
                  values +=[float(key)] * data[a][key][p]

    for p in projects:
        proj_name = p.replace('bpa-', '')
        if proj_name not in proj_names:
           proj_names.append(proj_name)
           values.append(None)

    df = pd.DataFrame(dict(x=values, g=proj_names))

    # Initialize the FacetGrid object
    sns.set(style="white", rc={"axes.facecolor": (0, 0, 0, 0)})
    pal = sns.cubehelix_palette(len(projects), rot=-.25, light=.7)
    g = sns.FacetGrid(df, row="g", hue="g", aspect=15, size=.5, palette=pal)

    # Draw the densities in a few steps
    #g.map(sns.distplot, "x", bins=50, kde=True)
    g.map(sns.kdeplot, "x", clip_on=False, shade=True, alpha=1, lw=1.5, bw=.2)
    g.map(sns.kdeplot, "x", clip_on=False, color="w", lw=2, bw=.2)
    g.map(plt.axhline, y=0, lw=2, clip_on=False)
    g.map(label, "x")

    # Set the subplots to overlap
    g.fig.subplots_adjust(hspace=-.25)

    # Remove axes details that don't play will with overlap
    g.set_titles("")
    g.set(xlabel="")
    g.set(yticks=[])
    g.despine(bottom=True, left=True)

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
     if isinstance(mtde_fields[mtde], list):
         for m in mtde_fields[mtde]:
            print "Getting %s values..." % m
            summary = query_mtde_field(m, mtde, projects, auth)
            data[m] = summary
     else:
         print "Getting %s values..." % (mtde)
         summary = query_mtde_field(mtde_fields[mtde], mtde, projects, auth)
         data[mtde] = summary


  output_matrix_table(data, projects, matrix_file_name)
  file_name = matrix_file_name

  if args.copy_file_to_server:
     print "Copying %s to %s" % (file_name, nginx_loc + file_name)
     shutil.copyfile(file_name, nginx_loc + file_name)
     for file in glob.glob('*.svg'):
         shutil.copy(file, nginx_loc)
     for file in glob.glob('*.tsv'):
         shutil.copy(file, nginx_loc)
