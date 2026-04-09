const express = require('express');
const cors = require('cors');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const amqp = require('amqplib');
const { Pool } = require('pg');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());

const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT) || 5432,
  database: process.env.DB_NAME || 'db_rekammedis',
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASS || 'postgres',
});

const packageDef = protoLoader.loadSync(path.join(__dirname, '../proto/patient.proto'));
const patientProto = grpc.loadPackageDefinition(packageDef).patient;

async function initDB(retries = 10) {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS rekam_medis (
        id SERIAL PRIMARY KEY,
        patient_id VARCHAR(100),
        name VARCHAR(200),
        diagnosis TEXT,
        dokter VARCHAR(200),
        catatan TEXT,
        tanggal_kunjungan DATE,
        draft_status VARCHAR(50) DEFAULT 'draft',
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`ALTER TABLE rekam_medis ADD COLUMN IF NOT EXISTS diagnosis TEXT`);
    await pool.query(`ALTER TABLE rekam_medis ADD COLUMN IF NOT EXISTS dokter VARCHAR(200)`);
    await pool.query(`ALTER TABLE rekam_medis ADD COLUMN IF NOT EXISTS catatan TEXT`);
    await pool.query(`ALTER TABLE rekam_medis ADD COLUMN IF NOT EXISTS tanggal_kunjungan DATE`);
    await pool.query(`ALTER TABLE rekam_medis ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()`);
    console.log('DB Service B siap');
  } catch (err) {
    if (retries > 0) {
      console.log(`DB belum siap, mencoba lagi dalam 3 detik... (${retries} sisa percobaan)`);
      await new Promise(resolve => setTimeout(resolve, 3000));
      return initDB(retries - 1);
    }
    throw err;
  }
}

// gRPC handler
function validateMedicalRecord(call, callback) {
  const { patientId, name } = call.request;
  console.log(`[gRPC] validateMedicalRecord → patient_id: ${patientId}, name: ${name}`);
  pool.query(
    'SELECT id FROM rekam_medis WHERE LOWER(name) = LOWER($1)',
    [name]
  ).then(result => {
    const sudahAda = result.rows.length > 0;
    console.log(`[gRPC] validateMedicalRecord → is_valid: ${!sudahAda}, message: ${sudahAda ? 'sudah punya rekam medis' : 'data valid'}`);
    callback(null, {
      is_valid: !sudahAda,
      message: sudahAda ? 'Pasien sudah punya rekam medis di Service B' : 'Data valid',
      draft_record_id: 'DRAFT-' + patientId,
    });
  }).catch(err => {
    console.error('[gRPC] validateMedicalRecord error:', err.message);
    callback(err);
  });
}

function checkPatientRecord(call, callback) {
  const { patientId } = call.request;
  console.log(`[gRPC] checkPatientRecord → patient_id: ${patientId}`);
  pool.query(
    'SELECT id FROM rekam_medis WHERE patient_id=$1',
    [patientId]
  ).then(result => {
    const hasRecord = result.rows.length > 0;
    console.log(`[gRPC] checkPatientRecord → has_record: ${hasRecord}`);
    callback(null, {
      has_record: hasRecord,
      message: hasRecord ? 'Pasien memiliki rekam medis' : 'Tidak ada rekam medis',
    });
  }).catch(err => {
    console.error('[gRPC] checkPatientRecord error:', err.message);
    callback(err);
  });
}

// RabbitMQ consumer
async function startConsumer() {
  try {
    const conn = await amqp.connect(process.env.RABBITMQ_URL || 'amqp://localhost');
    const channel = await conn.createChannel();
    await channel.assertQueue('patient.registered');
    channel.prefetch(1); // ambil 1 pesan dulu, sisanya tetap di queue
    console.log('Menunggu pesan dari RabbitMQ...');
    channel.consume('patient.registered', async (msg) => {
      if (msg) {
        const data = JSON.parse(msg.content.toString());
        console.log('Event diterima:', data.name);
        // Simulasi proses pengolahan data (agar terlihat di grafik RabbitMQ)
        await new Promise(resolve => setTimeout(resolve, 2000));
        await pool.query(
          'INSERT INTO rekam_medis (patient_id, name) VALUES ($1, $2)',
          [data.patientId, data.name]
        );
        console.log('Draf rekam medis dibuat untuk:', data.name);
        channel.ack(msg);
      }
    });
  } catch (err) {
    console.error('RabbitMQ error:', err.message);
    setTimeout(startConsumer, 3000);
  }
}

// POST - tambah rekam medis manual
app.post('/rekam-medis', async (req, res) => {
  const { patient_id, name, diagnosis, dokter, catatan, tanggal_kunjungan, draft_status } = req.body;
  if (!patient_id || !name) return res.status(400).json({ message: 'patient_id dan name wajib diisi' });
  const result = await pool.query(
    `INSERT INTO rekam_medis (patient_id, name, diagnosis, dokter, catatan, tanggal_kunjungan, draft_status)
     VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
    [patient_id, name, diagnosis || null, dokter || null, catatan || null, tanggal_kunjungan || null, draft_status || 'draft']
  );
  res.json(result.rows[0]);
});

// GET - semua rekam medis
app.get('/rekam-medis', async (req, res) => {
  const result = await pool.query('SELECT * FROM rekam_medis ORDER BY created_at DESC');
  res.json(result.rows);
});

// GET - draft rekam medis (belum ada diagnosis)
app.get('/rekam-medis/draft', async (req, res) => {
  const result = await pool.query(
    `SELECT * FROM rekam_medis WHERE diagnosis IS NULL OR diagnosis = '' ORDER BY created_at DESC`
  );
  res.json(result.rows);
});

// GET - rekam medis by patient_id
app.get('/rekam-medis/patient/:patient_id', async (req, res) => {
  const result = await pool.query(
    'SELECT * FROM rekam_medis WHERE patient_id = $1 ORDER BY created_at DESC',
    [req.params.patient_id]
  );
  res.json(result.rows);
});

// PUT - update rekam medis
app.put('/rekam-medis/:id', async (req, res) => {
  const { diagnosis, dokter, catatan, tanggal_kunjungan, draft_status } = req.body;
  const result = await pool.query(
    `UPDATE rekam_medis
     SET diagnosis=$1, dokter=$2, catatan=$3, tanggal_kunjungan=$4, draft_status=$5, updated_at=NOW()
     WHERE id=$6 RETURNING *`,
    [diagnosis || null, dokter || null, catatan || null, tanggal_kunjungan || null, draft_status || 'aktif', req.params.id]
  );
  if (result.rows.length === 0) return res.status(404).json({ message: 'Rekam medis tidak ditemukan' });
  res.json(result.rows[0]);
});

// DELETE - hapus rekam medis
app.delete('/rekam-medis/:id', async (req, res) => {
  const result = await pool.query('DELETE FROM rekam_medis WHERE id=$1 RETURNING id', [req.params.id]);
  if (result.rows.length === 0) return res.status(404).json({ message: 'Rekam medis tidak ditemukan' });
  res.json({ success: true, message: 'Rekam medis berhasil dihapus' });
});

// GET - stats
app.get('/stats', async (req, res) => {
  const total = await pool.query('SELECT COUNT(*) as total FROM rekam_medis');
  const draft = await pool.query("SELECT COUNT(*) as total FROM rekam_medis WHERE draft_status = 'draft'");
  const aktif = await pool.query("SELECT COUNT(*) as total FROM rekam_medis WHERE draft_status = 'aktif'");
  res.json({
    total_rekam_medis: parseInt(total.rows[0].total),
    draft: parseInt(draft.rows[0].total),
    aktif: parseInt(aktif.rows[0].total),
  });
});

function startGrpcServer() {
  const server = new grpc.Server();
  server.addService(patientProto.PatientService.service, { validateMedicalRecord, checkPatientRecord });
  server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), () => {
    console.log('gRPC Service B jalan di port 50051');
  });
}

initDB().then(() => {
  startGrpcServer();
  startConsumer();
  app.listen(3002, () => console.log('HTTP Service B jalan di port 3002'));
});
