const SETTINGS  = require('./settings.json');
const HANA      = require('./hana');
const FS        = require('fs');
const PATH      = require('path');





Object.keys(SETTINGS.jobs).map(job_name => {

    let job = SETTINGS.jobs[job_name];
    let sql = job.sql || job.file && FS.readFileSync(PATH.join('./', job.file), { encoding: 'utf8' })

    setInterval((name, query) => {

        HANA.dispatch(name, query)

    }, job.interval, job_name, sql)
})





process.on('SIGINT', function() {
    
    HANA.shutdown(() => {

        let reporFile = 'job_report_' + Date.now() + '.csv';
        let report = HANA.report();

        saveReport(report, './' + reporFile);
        console.log("\n\nJOB REPORT:", report);
        console.log(`Отчет сохранён в файле: ${reporFile}`);
        process.exit();
    })
});





function saveReport(report, path) {

    let data = [];
    let headers = undefined;

    Object.keys(report).map(k => {

        data.push("\r\n\r\n" + k);

        if (report[k].length > 0) {

            headers = Object.keys(report[k][0])

            data.push(headers.join(';'));

            report[k].map(j => {

                data.push(

                    headers.map(h => j[h]).join(';')
                );
            });
        }

        else {

            data.push("[EMPTY]\n");
        }
    });

    FS.writeFileSync(path, data.join("\r\n"));
}