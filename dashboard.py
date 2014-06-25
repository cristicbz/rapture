#!env python
# -*- coding: utf-8 -*-
import datetime
import json
from gevent import monkey
monkey.patch_all()
from bottle import route, run, template
from reinferio import jobs

TEMPLATE_DASHBOARD = r'''
<html>
<head>
<link rel='stylesheet' type='text/css'
href='//cdn.datatables.net/1.10.0/css/jquery.dataTables.css' />
<script src='//code.jquery.com/jquery-1.11.0.min.js'></script>
<script src='//cdn.datatables.net/1.10.0/js/jquery.dataTables.js'></script>
<script src='http://momentjs.com/downloads/moment.min.js'></script>
</head>
<body>

<h1>All jobs</h1>
<table id='all-jobs'>
<thead>
    <tr>
        <th class='time_created'>Time Created</th>
        <th class='time_updated'>Time Updated</th>
        <th class='job_type'>Type</th>
        <th class='status'>Status</th>
        <th class='job_id'>Job ID</th>
    </tr>
</thead>
<tbody>
</tbody>
</table>

<h1>In Progress</h1>
<table id='inprogress'>
<thead>
    <tr>
        <th class='time_updated'>Time Updated</th>
        <th class='job_type'>Type</th>
        <th class='job_id'>Job ID</th>
        <th class='progress'>Progress</th>
    </tr>
</thead>
<tbody>
</tbody>
</table>

</body>
<script>
function snapshotsToTableRows(snapshots, fields) {
    var rows = [];
    for (var i = 0; i < snapshots.length; ++i) {
        rows[i] = [];
        for (var j = 0; j < fields.length; ++j) {
            rows[i][j] = snapshots[i][fields[j]];
        }
    }
    return {'aaData': rows};
}

function renderStatus(status) {
    if (status === 'P') {
        return '<span style="color: #0000aa;">Pending...</span>'
    } else if (status === 'D') {
        return '<span style="color: #008800;">Done</span>'
    } else {
        return '<span style="color: #aa0000;">Failed</span>'
    }
}

function renderId(id) {
    return '<a href="job/' + id + '">' + id + '</a>';
}

function renderTime(timestamp) {
    return moment.unix(timestamp).format('HH:mm:ss / (DD MMM YY)');
}

function newDataTable(element, opts) {
    var preprocess = opts.preprocess;
    var endpoint = opts.endpoint;
    var interval = opts.interval;
    opts.preprocess = opts.endpoint = opts.interval = null;
    opts.ajax = endpoint;
    opts.fnServerData = function(sSource, aoData, fnCallback, oSettings) {
        oSettings.jqXHR = $.ajax({
            'dataType': 'json',
            'type': 'GET',
            'url': endpoint,
            'data': aoData,
            'success': function(json) {
                fnCallback(preprocess(json));
            }
        });
    }
    var table = element.DataTable(opts);

    if (interval != null) {
        setInterval(function() {
            table.ajax.reload(null, false);
            return true;
        }, interval);
    }

    return table;
}

$(document).ready(function() {
    newDataTable(
        $('#all-jobs'), {
            endpoint: 'api/all',
            interval: 1000,
            order: [[1, 'desc']],
            columnDefs: [
                { targets: ['status'], render: renderStatus },
                { targets: ['job_id'], render: renderId },
                { targets: ['time_created',
                            'time_updated'], render: renderTime }
            ],
            preprocess: function(json) {
                return snapshotsToTableRows(
                    json.jobs, ['time_created', 'time_updated', 'job_type',
                                'status', 'job_id']);
            }
        }
    );
    newDataTable(
        $('#inprogress'), {
            endpoint: 'api/inprogress',
            interval: 1000,
            order: [[0, 'desc']],
            columnDefs: [
                { targets: ['status'], render: renderStatus },
                { targets: ['job_id'], render: renderId },
                { targets: ['time_created',
                            'time_updated'], render: renderTime }
            ],
            preprocess: function(json) {
                return snapshotsToTableRows(
                    json.jobs, ['time_updated', 'job_type', 'job_id',
                                'progress']);
            }
        }
    );
});
</script>
</html>
'''

TEMPLATE_JOB_VIEW = r'''
<html>
<head>
<link rel='stylesheet' type='text/css'
href='//cdn.datatables.net/1.10.0/css/jquery.dataTables.css' />
<script src='//code.jquery.com/jquery-1.11.0.min.js'></script>
<script src='//cdn.datatables.net/1.10.0/js/jquery.dataTables.js'></script>
<script src='http://momentjs.com/downloads/moment.min.js'></script>
</head>
<body>
<a href='/dashboard'>&lt; Back to Dashboard</a>
<h1>Job: {{job.job_id}}</h1>
<table id='job-table'>
<thead>
    <tr>
        <th>Field</th>
        <th>Value</th>
    </tr>
</thead>
<tbody>
    % for field in job._fields:
    <tr><td>{{field}}</td><td><pre>{{job._asdict()[field]}}</pre></td></tr>
    % end
</tbody>
</table>
</body>
<script>
$(document).ready(function() {
    var table = $('#job-table').dataTable({})
});
</script>
</html>
'''

job_queue = None


@route('/')
@route('/dashboard')
def show_dashboard():
    return template(TEMPLATE_DASHBOARD)


def format_timestamp(timestamp):
    dt = datetime.datetime.fromtimestamp(float(timestamp))
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def ids_to_json(id_list):
    return json.dumps({'jobs': [job_queue.fetch_snapshot(job_id)._asdict()
                       for job_id in id_list]})


@route('/api/all')
def all_jobs():
    return ids_to_json(job_queue.debug_fetch_all_ids())


@route('/api/inprogress')
def inprogress_jobs():
    return ids_to_json(job_queue.debug_fetch_inprogress_ids())


@route('/job/<job_id:path>')
def job_snapshot(job_id):
    try:
        job = job_queue.fetch_snapshot(job_id)
    except:
        return

    return template(TEMPLATE_JOB_VIEW, job=job)



if __name__ == '__main__':
    job_queue = jobs.connect_to_queue()
    run(reloader=True, debug=True, server='gevent')
