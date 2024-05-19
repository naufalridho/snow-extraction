const fs = require('fs');
const mariadb = require('mariadb');
const {
    execSync
} = require('child_process');
const { createObjectCsvWriter } = require('csv-writer');

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
        const snowArchival = new SnowArchival(conn, '/mt/ebs/result');

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

    constructor(conn, resultDir) {
        this.conn = conn;
        this.resultDir = resultDir;
    }
    async start() {
        let tasks;
        // while (tasks > 0) {
        tasks = await this.getTasks(0, 2);
        console.log('task length', tasks.length)

        for (const task of tasks) {
            try {
                execSync(`mkdir -p ${this.getTaskPath(taskNumber)}`);
                await this.extractCsv(task);
                await this.extractAttachments(task);
            } catch (err) {
                console.error(`sys_id: ${task.sys_id}, task_number: ${task.number}, err:`, err);
            }
        }
        // }
    }

    async extractCsv(task) {
        
    }

    async getTasks(offset, limit) {
        //return this.conn.query(`
          // select * from task where sys_class_name = 'sc_req_item' order by number limit ${limit} offset ${offset};
            // `);
        return this.conn.query(`
    	  select * from task where number = 'RITM0010022';
        `);
    }

    async extractAttachments(task) {
        const chunks = await this.getChunks(task.sys_id);

        this.groupChunksIntoAttachments(chunks).forEach(a =>
            this.extractAttachment(task.number, a);
        );
    }

    getChunks(sysId) {
        return this.conn.query(`select sa.file_name as file_name, sa.compressed as compressed, sad.data as data
          from sys_attachment sa join sys_attachment_doc sad on sa.sys_id = sad.sys_attachment and sa.table_sys_id = '${sysId}'
          order by sad.position;
        `);
    }

    groupChunksIntoAttachments(chunks) {
        const grouped = chunks.reduce((acc, chunk) => {
            if (!acc[chunk.file_name]) {
                acc[chunk.file_name] = {chunks: []};
            }
            acc[chunk.file_name].chunks.push(chunk);
            return acc;
        }, {});
        const res = Object.values(grouped);

    	return res;
    }

    extractAttachment(taskNumber, attachment) {
    	const base64Chunks = attachment.chunks.map(chunk => chunk.data);

        const concatenatedBuffer = this.decodeMultipartBase64(base64Chunks);
        const meta = attachment.chunks[0];

        const attachmentFilePath = `\"${this.getTaskPath(taskNumber)}/${meta.file_name}\"`;


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
        fs.writeFileSync('tmp.gz', buf);
        execSync(`gzip -d tmp.gz && mv tmp ${filepath}`);
    }

    writeFile(filepath, buf) {
        fs.writeFileSync(filepath, buf);
    }

    getTaskPath(taskNumber) {
        return `${this.resultDir}/${taskNumber}`
    }
}

main();
