#!/usr/bin/env python

import os, sys
from occlibs.s3_wrapper import S3_Wrapper
from argparse import ArgumentParser
from collections import defaultdict
import shutil
import datetime

node_names = [
    'aliquot',
    'assay_result',
    'case',
    'experiment',
    'experimental_metadata',
    'experimental_strategy',
    'project',
    'read_group',
    'sample',
    'sample_expectation',
    'slide',
    'slide_image',
    'slide_count'
]


main_header_order = [
    'project',
    'experiment',
    'case',
    'clinical',
    'sample',
    'sample_expectation',
    'slide',
    'slide_image',
    'slide_count',
    'aliquot',
    'read_group',
    'assay_result',
    'sequencing_files',
    'experimental_analysis',
    'experimental_metadata',
    'formats_not_supported',
    'validated'
]
header_strings = {
    'organization': 'Organization',
    'project': 'Project',
    'experiment': 'Experiments',
    'case': 'Cases',
    'clinical': 'Clinical Data',
    'sample': 'Samples',
    'sample_expectation': 'Expected Mutations',
    'slide': 'Slides',
    'slide_image': 'Slide Images',
    'slide_count': 'Slide Counts',
    'aliquot': 'Aliquots',
    'read_group': 'Read Groups',
    'assay_result': 'Assay Results',
    'sequencing_files': 'Sequencing Files',
    'experimental_analysis': 'Experimental Analysis',
    'experimental_result': 'Experimental Results',
    'experimental_metadata': 'Experimental Metadata',
    'formats_not_supported': 'Formats Not Supported',
    'validated': 'Validated*'
}

non_total_columns = [
    'validated',
    'formats_not_supported'
]

clinical = [
    'demographic',
    'family_history',
    'exposure',
    'diagnosis',
    'treatment'
]

sequencing = [
    'submitted_unaligned_reads',
    'submitted_aligned_reads',
    'submitted_somatic_mutations',
    'submitted_copy_number_files'
]

sum_columns = {
    'clinical': clinical,
    'sequencing_files': sequencing
}

potential_names = {
    'aliquot':
        ['aliquots'],
    'assay_result':
        ['assay-result'],
    'case':
        ['cases'],
    'clinical':
        [],
    'diagnosis':
        [],
    'demographic':
        ['demographics'],
    'experiment':
        ['experiments', 'experements'],
    'experimental_analysis':
        ['experimental-analysis',
         'experiment-analysis',
         'experiment_analysis'],
    'experimental_metadata':
        ['experiment-metadata',
         'experimental-metadata',
         'experiment_metadata'],
    'experimental_result':
        ['experimental-result',
         'experiment-result',
         'experiment_result'],
    'experimental_strategy':
        ['experimental-strategy',
         'experimental-strategies',
         'experimental_strategies'],
    'exposure':
        ['exposures'],
    'family_history':
        ['family-history',
         'family-histories',
         'family_histories'],
    'project':
        ['projects'],
    'read_group':
        ['read-group',
         'read-groups',
         'read_groups'],
    'sample':
        ['samples'],
    'sample_expectation':
        ['sample-expectation',
         'sample-expectations',
         'sample_expectations'],
    'sequencing_files':
        [],
    'slide':
        ['slides'],
    'slide_image':
        ['slide-image',
         'slide-images',
         'slide_images'],
    'slide_count':
        ['slide-count',
         'slide-counts',
         'slide_counts'],
    'submitted_unaligned_reads':
        ['submitted-unaligned-reads',
         'submitted-unaligned-read',
         'submitted_unaligned_read'],
    'submitted_aligned_reads':
        ['submitted-aligned-reads',
         'submitted-aligned-read',
         'submitted_aligned_read'],
    'submitted_somatic_mutations':
        ['submitted-somatic-mutation',
         'submitted-somatic-mutations',
         'submitted_somatic_mutation'],
    'submitted_copy_number_files':
        ['submitted-copy-number-files',
         'submitted-copy-number-file',
         'submitted_copy_number_file'],
    'slide_image':
        ['slide-image',
        'slide_images',
        'slide-images'],
    'treatment':
        ['treatments']
}

no_total_columns = [
    'formats_not_supported'
]

# secondary matrix data
matrix_table_lookup = {
    'assay_result': ['assay_kit_name', 'ctc_feature_value', 'assay_technology'],
    'experimental_analysis': 
        ['sensitivity', 'LLOD', 'specificity'],
    #'read_group': ,
    'sample': ['volume', 'method_of_sample_procurement']
}

json_to_logical_value = {
    'assay_kit_name': 'extraction_method',
    'assay_technology': 'technology_used',
    'ctc_feature_value': 'ctdna_concentration',
    'sensitivity': 'sensitivity',
    'LLOD': 'llod',
    'specificity': 'specificity',
    'method_of_sample_procurement': 'collection_method',
    'volume': 'volume'
}

secondary_header_order = [
    'organization',
    'project',
    'description',
    'collection_method',
    'technology_used',
    'extraction_method',
    'volume',
    'ctdna_concentration',
    'llod',
    'specificity',
    'sensitivity'
]

secondary_header_strings = {
    'organization': 'Organization',
    'project': 'Project',
    'description': 'Project Description',
    'extraction_method': 'Extraction Method',
    'technology_used': 'Technology Used',
    'ctdna_concentration': 'ctDNA Concentration',
    'sensitivity': 'Sensitivity',
    'llod': 'LLOD',
    'specificity': 'Specificity',
    'collection_method': 'Collection Method',
    'volume': 'Volume'
}


file_type = 'tsv'
validation_file = 'validated.status'
validated_key = 'validated'

hardcoded_orgs = {
    #'Foundation Medicine P0001': '',
    #'PersonalGenome Beta1': 'FastQ w/o Metadata',
    'MSKCC P0001': 'Unsupported TSV'
}

def parse_cmd_args(s3_inst):
    parser = ArgumentParser(s3_inst)
    parser.add_argument('--copy_file_to_server',
                        help='copies file to object store',
                        action='store_true')
    parser.add_argument('--create_secondary_matrix',
                        help='create the secondary matrix',
                        action='store_true')

    parser.set_defaults(print_list=False)
    args = parser.parse_args()
    
    return args

def parse_data_file(file_data=None,
                    data_type=None,
                    custom_delimiter=None):
    """Processes loaded data as a tsv, csv, or
    json, returning it as a list of dicts"""
    key_data = []
    header = None
    skipped_lines = 0
    delimiters = {  'tsv': '\t',
                    'csv': ',',
                    'json': '',
                    'other': ''}
    other_delimiters = [' ', ',', ';']


    if data_type not in delimiters.keys():
        print "Unable to process data type %s" % data_type
        print "Valid data types:"
        print delimters.keys()
    else:
        if data_type == 'other':
            if custom_delimiter:
                delimiter = custom_delimiter
            else:
                print "With data_type 'other', a delimiter is needed"
                raise
        else:
            delimiter = delimiters[data_type]

        if data_type == 'json':
            for line in file_data.split('\n'):
                line_data = json.loads(line)
                key_data.append(line_data)
        # load as tsv/csv, assuming the first row is the header
        # that provides keys for the dict
        else:
            for line in file_data.split('\n'):
                if delimiter in line:
                    if len(line.strip('\n').strip()):
                        if not header:
                            header = line.strip('\n').split(delimiter)
                        else:
                            line_data = dict(zip(header, line.strip('\n')\
                                                        .split(delimiter)))
                            key_data.append(line_data)
                else:
                    # ok, let's see if we can be smart here
                    #if not header:
                    #    remaining_chars = set([c for c in line if not c.isalnum()])
                    skipped_lines += 1

    print '%d lines in file, %d processed' % (len(file_data.split('\n')), len(key_data))
    return key_data

def normalize_node_name(key_name):
   
    node_name = None
    # get node name from key name
    file_name = key_name.split('/')[-1]
    if file_name.count('.') > 1:
        file_name = '.'.join(file_name.split('.')[1:])

    file_name = file_name.lower()
    potential_node_name = file_name.split('.')[0]
    # looking for tails, like with UMich
    if any(char.isdigit() for char in potential_node_name):
        potential_node_name = '_'.join(potential_node_name.split('_')[:-1])
    #print potential_node_name

    # find node if in our table
    if potential_node_name in potential_names.keys():
        node_name = potential_node_name
    # if not, let's try and see if it's
    # something we know about
    else:
        for node in potential_names.keys():
            if potential_node_name in potential_names[node]:
                node_name = node

    return node_name

def parse_org_project(value):
    data = {}
    project = value['project'][0]
    #print project
    delimeters = ['_', '-']
    delimeter = None
    for delim in delimeters:
        if delim in project['submitter_id']:
            delimeter = delim
            break
    
    if delimeter:
        project_parts = project['submitter_id'].split(delimeter)
    else:
        print 'Unable to guess delimeter for {}'.format(project['submitter_id'])
    data['organization'] = project_parts[1]
    project_name = None
    if len(project_parts) > 2:
        for part in project_parts:
            if part.startswith('P'):
                project_name = part
    
    if project_name:
        data['project'] = project_name
    else:
        print 'Unable to determine project for {}'.format(project['submitter_id'])
        data['project'] = 'p0001'

    if 'name' in project:
        data['description'] = project['name']
    else:
        data['description'] = 'unknown'

    return data


def output_detailed_matrix_table(data, file_name):
    print '***Writing detailed HTML out***'
    with open(file_name, 'w') as out_file:
        out_file.write('<html>\n')
        out_file.write('<head>\n')
        out_file.write('<link rel="stylesheet" type="text/css" href="style.css">\n')
        out_file.write('<title>Blood Profiling Atlas Project Detailed Data Matrix</title>\n')
        out_file.write('<link rel="shortcut icon" href="favicon.ico" type="image/x-icon"/>\n')
        out_file.write('</head>\n')
        out_file.write('<body>\n')
        out_file.write('<div class="master-content">\n')
        out_file.write('<div class="container">\n')
        out_file.write('<div class="grid">\n')
        out_file.write('<section class="grid__col--60 grid__col--push10" role="header" id="header">\n')
        out_file.write('<h1><img src="bpa-logo.png" height="64">Blood Profiling Atlas Project Detailed Data Matrix</h1>\n')
        out_file.write('<h2>Mouse over "*" for details</h2>')
        out_file.write('</section>\n')
        out_file.write('<section class="grid__col--60 grid__col--push10" role="main" id="table">\n')
        out_file.write('<table style = "width:100%">\n')
        header_order = None
        for key in sorted(data.keys()):
            if not header_order:
                header_order = list(secondary_header_order)
                out_file.write('<thead>\n')
                for header_val in header_order:
                    out_file.write('<th>%s</th>' % secondary_header_strings[header_val])
                out_file.write('</thead>\n')
            out_file.write('<tr>\n')
            out_file.write('<th>%s</th>' % ' '.join(key.replace('_', ' ').split()[1:2]))
            if 'organization' in header_order:
                header_order.remove('organization')
            for header_val in header_order:
                if header_val in data[key].keys():
                    val2 = data[key][header_val]
                    if val2:
                        if ((header_val != 'project') and 
                            (header_val != 'description')):
                            if len(val2) > 1:
                                count = 0
                                values = ''
                                for val in sorted(val2):
                                    if ((not (count % 8)) and count):
                                        if len(values):
                                            values += (', ' + val)
                                        else:
                                            values = val
                                    else:
                                        if len(values):
                                            values += (',' + val)
                                        else:
                                            values = val
                                    count += 1
                                out_file.write('<td><a class="tooltip" href="#">*<span class="classic">{}</span></a></td>'.format(values))
                            else:
                                val = list(val2)[0]
                                out_file.write('<td>{}</td>'.format(val))
                        else:
                            out_file.write('<td>{}</td>'.format(val2))

                    else:
                        out_file.write('<td>--</td>' )
                else:
                    #print "%s not in data" % header_val
                    #print data.keys()
                    if header_val == 'format_not_supported':
                        out_file.write('<td>N/A</td>')
                    else:
                        out_file.write('<td>--</td>' )

            out_file.write('</tr>\n')
            
        out_file.write('</table></section></div></div></div>\n')
        out_file.write('Last processed: %s UTC\n' % datetime.datetime.today().isoformat())
        out_file.write('</body></html>\n')

def output_main_matrix_table(data, file_name):
    print '***Writing HTML out***'
    with open(file_name, 'w') as out_file:
        out_file.write('<html>\n')
        out_file.write('<head>\n')
        out_file.write('<link rel="stylesheet" type="text/css" href="style.css">\n')
        out_file.write('<title>Blood Profiling Atlas Project Data Matrix - Submitted Data Counts</title>\n')
        out_file.write('<link rel="shortcut icon" href="favicon.ico" type="image/x-icon"/>\n')
        out_file.write('</head>\n')
        out_file.write('<body>\n')
        out_file.write('<div class="master-content">\n')
        out_file.write('<div class="container">\n')
        out_file.write('<div class="grid">\n')
        out_file.write('<section class="grid__col--60 grid__col--push10" role="header" id="header">\n')
        out_file.write('<h1><img src="bpa-logo.png" height="64">Blood Profiling Atlas Project Data Matrix</h1>\n')
        out_file.write('<h2>Submitted Data Counts</h2>')
        out_file.write('</section>\n')
        out_file.write('<section class="grid__col--60 grid__col--push10" role="main" id="table">\n')
        out_file.write('<table style = "width:100%">\n')
        header_order = None
        totals = defaultdict(int)
        for key in sorted(data.keys()):
            value = data[key]
            if not header_order:
                header_order = list(main_header_order)
                out_file.write('<thead>\n')
                out_file.write('<th class="organization">%s</th>' % header_strings['organization'])
                for header_val in header_order:
                    #print header_val
                    out_file.write('<th>%s</th>' % header_strings[header_val])
                out_file.write('</thead>\n')
            out_file.write('<tr>\n')
            org_data = parse_org_project(data[key])
            out_file.write('<th>%s</th>' % ' '.join(key.replace('_', ' ').split()[1:2]))
            for header_val in header_order:
                if header_val in data[key].keys():
                    val2 = data[key][header_val]
                    if header_val == validated_key:
                        out_file.write('<td>%r</td>' % val2)
                        if val2:
                            totals[header_val] += 1
                    else:
                        if val2:
                            if header_val != 'project':
                                out_file.write('<td>%d</td>' % len(val2))
                            else:
                                out_file.write('<td>%s</td>' % org_data['project'])
                                
                            totals[header_val] += len(val2)
                        else:
                            out_file.write('<td>--</td>' )
                else:
                    #print "%s not in data" % header_val
                    #print data.keys()
                    if header_val == 'format_not_supported':
                        out_file.write('<td>N/A</td>')
                    else:
                        out_file.write('<td>--</td>' )

            out_file.write('</tr>\n')
            
        # write the hard coded XML folks
        for org, val in hardcoded_orgs.iteritems():
            out_file.write('<tr>\n')
            out_file.write('<th>%s</th>' % org)
            for header_val in header_order:
                if header_val == 'formats_not_supported':
                    out_file.write('<td>%s</td>' % val)
                elif header_val == validated_key:
                    out_file.write('<td>False</td>')
                else:
                    out_file.write('<td>--</td>')

            out_file.write('</tr>\n')

        out_file.write('<tfoot>\n')
        out_file.write('<th>TOTALS</th>')
        for header_val in header_order:
            if header_val in no_total_columns:
                out_file.write('<td>N/A</td>')
            else:
                if header_val in totals:
                    val = totals[header_val]
                else:
                    val = 0
                out_file.write('<td>%d</td>' % val)
        out_file.write('</tfoot>\n')

        out_file.write('</table></section></div></div></div>\n')
        out_file.write('Last processed: %s UTC\n' % datetime.datetime.today().isoformat())
        out_file.write('<h2><p>* - data model fit + upload complete</h2>\n')
        out_file.write('</body></html>\n')


def sum_parsed_data(data):
    new_dict = {}
    for key, value in data.iteritems():
        #print '\n***Processing {}'.format(key)
        new_dict[key] = {}
        for key2, val2 in value.iteritems():
            if key2 != validated_key:
                #print 'loading {} of {}'.format(len(value[key2]), key2)
                if key2 in new_dict[key]:
                    new_val = list(new_dict[key][key2])
                else:
                    new_val = list(value[key2])
                for sum_key, sum_val in sum_columns.iteritems():
                    if key2 in sum_val:
                        if len(val2):
                            #print 'extending %s by %d' % (sum_key, len(val2))
                            new_dict[key][sum_key] = list(val2)
                new_dict[key][key2] = list(new_val)
                #print '%s %s now %d' % (key, key2, len(new_val))
            else:
                new_dict[key][key2] = bool(val2)

        #for key2 in new_dict[key].keys():
        #    if type(new_dict[key][key2]) != bool:
        #        print "{}: {}".format(key2, len(new_dict[key][key2]))

    return new_dict



def process_parsed_data(data):
    new_dict = {}
    for key, value in data.iteritems():
        data = {}
        project = value['project'][0]
        print project
        delimeters = ['_', '-']
        delimeter = None
        for delim in delimeters:
            if delim in project['submitter_id']:
                delimeter = delim
                break
        
        if delimeter:
            project_parts = project['submitter_id'].split(delimeter)
        else:
            print 'Unable to guess delimeter for {}'.format(project['submitter_id'])
        #data['organization'] = project_parts[1]
        project_name = None
        if len(project_parts) > 2:
            for part in project_parts:
                if part.startswith('P'):
                    project_name = part
        
        if project_name:
            data['project'] = project_name
        else:
            print 'Unable to determine project for {}'.format(project['submitter_id'])
            data['project'] = 'p0001'

        if 'name' in project:
            data['description'] = project['name']
        else:
            data['description'] = 'unknown'

        for mat_key, mat_val in matrix_table_lookup.iteritems():
            for entry in value[mat_key]:
                for entry2 in mat_val:
                    if entry2 in entry:
                        if json_to_logical_value[entry2] not in data:
                            data[json_to_logical_value[entry2]] = set([entry[entry2]])
                        else:
                            data[json_to_logical_value[entry2]].add(entry[entry2])
        new_dict[key] = data

    for key, value in new_dict.iteritems():
        print key
        for key2, val2 in value.iteritems():
            print key2, val2
        print
    return new_dict


def print_dict(cur_dict):
    for key, value in cur_dict.iteritems():
        print key
        for key2, val2 in value.iteritems():
            if val2:
                if type(val2) == list:
                    print "\t%s: %d" % (key2, len(val2))
                else:
                    print "\t%s: %r" % (key2, val2)
                #print val2



s3_inst = S3_Wrapper()
args = parse_cmd_args(s3_inst)

object_store = os.environ['S3_OBJECT_STORE']
bucket_name = os.environ['S3_BUCKET']

s3_conn = s3_inst.connect_to_s3(object_store)
files = s3_inst.get_files_in_s3_bucket(s3_conn, bucket_name)



default_data = {}
for val in potential_names.keys():
    default_data[val] = []
default_data[validated_key] = False

total_size = 0
all_org_data = {}
for entry in files:
    # check if it's a tsv file
    if file_type in entry['key_name']:
        # check if it's a tsv file we care about
        org_name = entry['key_name'].split('/')[0]
        node_data_type = normalize_node_name(entry['key_name'])
        if not node_data_type:
            print "Unable to figure out data type for %s, skipping" % entry['key_name']
        else:
            # load the data
            print "Loading %s" % entry['key_name']
            data = s3_inst.load_file(conn=s3_conn,
                                     bucket_name=bucket_name,
                                 key_name=entry['key_name'])

            file_data = parse_data_file(data, file_type)

            if org_name not in all_org_data:
                all_org_data[org_name] = dict(default_data)
            if all_org_data[org_name][node_data_type]:
                print "Warning, overwriting existing data for %s" % node_data_type
            all_org_data[org_name][node_data_type] = file_data
    elif validation_file in entry['key_name']:
        print "Validation file found"
        if org_name not in all_org_data:
            all_org_data[org_name] = dict(default_data)
        all_org_data[org_name][validated_key] = True


matrix_file_name = 'matrix.html'
matrix_2_file_name = 'matrix2.html'
nginx_loc = '/usr/share/nginx/html/'

if not args.create_secondary_matrix:
    new_org_data = sum_parsed_data(all_org_data)
    #org_name = 'BPA_PersonalGenome_Beta1'
    #print '\n***Totals for {}***'.format(org_name)
    #for key in new_org_data[org_name].keys():
    #    if type(new_org_data[org_name][key]) != bool:
    #        print key, len(new_org_data[org_name][key])
    output_main_matrix_table(new_org_data, matrix_file_name)
    file_name = matrix_file_name
else:
    matrix_2_data = process_parsed_data(all_org_data)
    output_detailed_matrix_table(matrix_2_data, matrix_2_file_name)
    file_name = matrix_2_file_name

if args.copy_file_to_server:
    print "Copying %s to %s" % (file_name,
        nginx_loc + file_name)
    shutil.copyfile(file_name, nginx_loc + file_name)

#print_dict(new_org_data)
