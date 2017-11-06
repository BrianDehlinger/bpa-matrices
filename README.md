# bpa-matrices: BloodPAC summary matrices #

Code to generate matrices for the BPA matrices

These Python scripts collect metadata and generate summary matrices from data submitted to the BloodPAC Commons. For this purpose, the different scripts make different calls to the query API through GraphQL queries to get the needed data. The four scripts are run periodically (~each hour) in a server using cron to keep the summary matrices updated.

# List of summary matrices #

* _matrix\_count\_api.py_: General summary counts of different entities in the submitted projects: #studies, #cases, #biospecimens, #samples, #assays, #files, etc.

* _matrix\_mtde\_api.py_: Several summary matrices showing the Minimal Technical Data Elements (MTDE) properties required for all projects across the BloodPAC Commons. These matrices include both tabular and graphical representations.

