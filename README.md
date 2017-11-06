# bpa-matrices: BloodPAC summary matrices #

These Python scripts collect metadata and generate summary matrices from data submitted to the BloodPAC Commons. For this purpose, the different scripts make different calls to the query API through GraphQL queries to get the needed data. The four scripts are run periodically (~each hour) in a server using cron to keep the summary matrices updated. In order to have a complete knowledge of the information that can be queried, we suggest to visit the [BloodPAC data dictionary](https://github.com/occ-data/bpadictionary).

## List of summary matrices ##

* _matrix\_count\_api.py_: General summary counts of different entities in the submitted projects: #studies, #cases, #biospecimens, #samples, #assays, #files, etc.

<https://services.bloodpac.org/matrix_api.html>

* _matrix\_mtde\_api.py_: Several summary matrices showing the Minimal Technical Data Elements (MTDE) properties required for all projects across the BloodPAC Commons. These matrices include both tabular and graphical representations.

<https://services.bloodpac.org/matrix_mtde.html>

* _matrix\_assay\_api.py_: Summary matrix collecting information associated to the studies performing any type of assay not directly related to sequencing: immunoassays, quantification assay, PCR, etc.

<https://services.bloodpac.org/matrix_assays.html>

* _matrix\_exp\_api.py_: Summary matrix collecting technical information associated to sequencing experiments performed across the different BloodPAC projects.

<https://services.bloodpac.org/matrix3.html>
