const fs = require('fs');
const mariadb = require('mariadb');
const {
    execSync,
    exec
} = require('child_process');

async function main() {
    console.log('Script started')

    let conn;
    try {
        const pool = mariadb.createPool({
            host: 'localhost',
            port: 3306,
            user: 'pmifsm',
            password: 'pmifsm',
            connectionLimit: 5,
            database: 'pmifsm'
        });

        conn = await pool.getConnection();
        const snowArchival = new SnowArchival(conn, '/mt/ebs/result', 1000);

        await snowArchival.start();
	    console.log('Script finished');
    } catch (err) {
        console.log(err);
    } finally {
        if (conn) await conn.end();
    }
}

class SnowArchival {
    conn;
    resultDir;
    batchSize;

    excludedRitms = [
        '2437ffc5478b295047f2c071e36d43df',
        '43cd2bd5478f695047f2c071e36d43e0',
        'b89ae391478f695047f2c071e36d436d',
        'd630e4d51b0ba550b3f5a6c3b24bcbe0',
        'ff4f94951b0ba550b3f5a6c3b24bcb76',
    ]

    constructor(conn, resultDir, batchSize) {
        this.conn = conn;
        this.resultDir = resultDir;
        this.batchSize = batchSize;
    }
    async start() {
        let startIdx = 0;

        while (true) {
            let tasks = await this.getTasks(startIdx, this.batchSize);
            if (tasks.length === 0) break;

            if (startIdx === 0) {
                tasks = tasks.filter(t => !this.excludedRitms.includes(t.sys_id))
            }

            startIdx += this.batchSize;

            const groupPath = this.getGroupPath(tasks);
            console.log(groupPath, startIdx)

            for (const task of tasks) {
                try {
                    const taskPath = this.getTaskPath(groupPath, task)
                    execSync(`mkdir -p ${taskPath}`);
                    await this.extractCsv(task, taskPath);
                    await this.extractAttachments(task, taskPath);
                } catch (err) {
                    console.error(`sys_id: ${task.sys_id}, task_number: ${task.number}, err:`, err);
                }
            }
        }
    }

    async extractCsv(task, taskPath) {
        const journals = await this.conn.query(`select * from sys_journal_field where element in ('work_notes', 'comments') and element_id = '${task.number}' order by sys_created_on;`);
        const commentsAndWorkNotes = journals.map(this.constructJournal).join('\n');

        const assignedTo = await this.getAssignedTo(task);
        const catItemName = await this.getCatItemName(task);

        const reference = await this.getReference(task);

        const context = await this.conn.query(`select name, stage from wf_context where sys_id = '${task.sys_id}'`);
        const stage = await this.conn.query(`select name from wf_stage where sys_id = '${context.stage}'`)

        const data = {
            'Number': task.number,
            'Cat Item': catItemName,
            'Watch List': task.a_str_24,
            'Stage': stage.name,
            'Approval Attachment': '',
            'State': task.state,
            'U Approval Request': task.a_str_11,
            'Approval Set': task.approval_set,
            'Assigned To': assignedTo,
            'Assignment Group': task.assignment_group,
            'Short Description': task.short_description,
            'Resolved By': assignedTo,
            'Closed Time': task.u_closed_time,
            'Resolution Note': task.a_str_10,
            'Closed At': task.closed_at,
            'Comments And Work Notes': commentsAndWorkNotes,
            'Contact Person': task.a_str_28,
            'Request': task.a_str_2, 
            'Reopen Count': '', 
            'Reference 1': reference,
            'Sys Created By': task.sys_created_by,
            'Reassignment Count': task.reassignment_count,
            'Generic Mailbox': task.a_str_23,
            'Cc': task.a_str_24, 
            'To Address': task.a_str_25,
            'Opened At': task.opened_at,
            'External Users Email': task.a_str_7,
            'Approval': task.approval,
            'Contact Type': task.contact_type,
            'Ritm Region': task.a_str_27,
            'Ritm Source': task.a_str_22,
            'Priority': task.priority,
            'State': task.state
        }

        const header = Object.keys(data).join(',');
        const values = Object.values(data).join(',');
        
        // Write CSV string to file
        // const filepath = `\"${taskPath}/${task.number}.csv\"`
        // fs.writeFileSync('data.csv', `${header}\n${values}`);
        // execSync(`mv data.csv ${filepath}`);
    }

    async getAssignedTo(task) {
        const user = await this.conn.query(`select name from sys_user where sys_id = '${task.a_ref_10}'`);
        return user.name;
    }

    async getCatItemName(task) {
        const cat = await this.conn.query(`select name from sc_cat_item where sys_id = '${task.a_ref_1}'`);
        return cat.name;
    }

    async getReference(task) {
        const refTask = await this.conn.query(`select number from task where sys_id = '${task.a_ref_9}'`);
        return refTask.number;
    }

    constructJournal(j) {
        return '${j.sys_created_by}\n${j.sys_created_on}\n${j.value}';
    }

    async getTasks(offset, limit, taskNumber) {
        if (taskNumber) {
            return this.conn.query(`
              select * from task where number = ${taskNumber};
            `);
        }

        return this.conn.query(`
          select * from task where sys_class_name = 'sc_req_item' order by number limit ${limit} offset ${offset};
        `);
        
    }

    async extractAttachments(task, taskPath) {
        const chunks = await this.getChunks(task.sys_id);

        this.groupChunksIntoAttachments(chunks).forEach(a =>
            this.extractAttachment(a, taskPath)
        );
    }

    getChunks(sysId) {
        return this.conn.query(`select sad.sys_attachment as sys_attachment_id, sa.file_name as file_name, sa.compressed as compressed, sad.data as data
        from sys_attachment sa join sys_attachment_doc sad on sa.sys_id = sad.sys_attachment and sa.table_sys_id = '${sysId}'
        order by sad.position;
      `);
    }

    groupChunksIntoAttachments(chunks) {
        const grouped = chunks.reduce((acc, chunk) => {
            if (!acc[chunk.sys_attachment_id]) {
                acc[chunk.sys_attachment_id] = {chunks: []};
            }
            acc[chunk.sys_attachment_id].chunks.push(chunk);
            return acc;
        }, {});
        const res = Object.values(grouped);

    	return res;
    }

    extractAttachment(attachment, taskPath) {
    	const base64Chunks = attachment.chunks.map(chunk => chunk.data);

        const concatenatedBuffer = this.decodeMultipartBase64(base64Chunks);
        const meta = attachment.chunks[0];

        const attachmentFilePath = `\"${taskPath}/${meta.file_name}\"`;

        if (meta.compressed > 0) {
            this.writeCompressedFile(attachmentFilePath, concatenatedBuffer);
        } else {
            this.writeFile(attachmentFilePath, concatenatedBuffer);
        }
    }

    decodeMultipartBase64(base64Chunks) {
        const binaryChunks = base64Chunks.map(chunk => Buffer.from(chunk, 'base64'));
        return Buffer.concat(binaryChunks);
    }

    writeCompressedFile(filepath, buf) {
        try { execSync('rm tmp', { stdio: [] })} catch (e) {};
        fs.writeFileSync('tmp.gz', buf);
        execSync(`gzip -d tmp.gz && mv tmp ${filepath}`);
    }

    writeFile(filepath, buf) {
        fs.writeFileSync(filepath, buf);
    }

    getTaskPath(groupPath, task) {
        return `${groupPath}/${task.number}/${this.formatDateWithTime(task.sys_created_on)}`
    }

    getGroupPath(tasks) {
        const startTask = tasks[0];
        const endTask = tasks[tasks.length - 1]
        return `${this.resultDir}/${startTask.number}-${endTask.number}_${this.formatDate(startTask.sys_created_on)}_${this.formatDate(endTask.sys_created_on)}`
    }

   formatDate(date) {
        const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
        const day = String(date.getDate()).padStart(2, '0');
        const month = months[date.getMonth()];
        const year = date.getFullYear();
        return `${day}${month}${year}`;
    }

    formatDateWithTime(date) {
        const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
        const day = String(date.getDate()).padStart(2, '0');
        const month = months[date.getMonth()];
        const year = date.getFullYear();
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        return `${day}${month}${year}_${hours}${minutes}`;
    }
}

main();
