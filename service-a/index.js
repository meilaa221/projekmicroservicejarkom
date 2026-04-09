const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const amqp = require('amqplib');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());

const pool = new Pool({
  host: process.env.DB_HOST || '127.0.0.1',
  port: parseInt(process.env.DB_PORT) || 5432,
  database: process.env.DB_NAME || 'db_pendaftaran',
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASS || 'postgres',
});

const packageDef = protoLoader.loadSync(path.join(__dirname, '../proto/patient.proto'));
const patientProto = grpc.loadPackageDefinition(packageDef).patient;
const grpcClient = new patientProto.PatientService(
  `${process.env.GRPC_HOST || 'localhost'}:50051`,
  grpc.credentials.createInsecure()
);

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pasien (
      id SERIAL PRIMARY KEY,
      name VARCHAR(200) NOT NULL,
      no_ktp VARCHAR(20),
      no_hp VARCHAR(15),
      jenis_kelamin VARCHAR(10),
      golongan_darah VARCHAR(5),
      tanggal_lahir DATE,
      alamat TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `);
  await pool.query(`ALTER TABLE pasien ADD COLUMN IF NOT EXISTS no_ktp VARCHAR(20)`);
  await pool.query(`ALTER TABLE pasien ADD COLUMN IF NOT EXISTS no_hp VARCHAR(15)`);
  await pool.query(`ALTER TABLE pasien ADD COLUMN IF NOT EXISTS jenis_kelamin VARCHAR(10)`);
  await pool.query(`ALTER TABLE pasien ADD COLUMN IF NOT EXISTS golongan_darah VARCHAR(5)`);
  console.log('DB Service A siap');
}

async function publishEvent(data) {
  try {
    const conn = await amqp.connect(process.env.RABBITMQ_URL || 'amqp://localhost');
    const channel = await conn.createChannel();
    await channel.assertQueue('patient.registered');
    channel.sendToQueue('patient.registered', Buffer.from(JSON.stringify(data)));
    console.log(`[RabbitMQ] Publish → queue: patient.registered, name: ${data.name}`);
    await channel.close();
    await conn.close();
  } catch (err) {
    console.error('[RabbitMQ] Publish error:', err.message);
  }
}

// POST - daftar pasien baru
app.post('/patients', async (req, res) => {
  const { name, no_ktp, no_hp, jenis_kelamin, golongan_darah, tanggal_lahir, alamat } = req.body;

  if (!name || !tanggal_lahir || !alamat) {
    return res.status(400).json({ success: false, message: 'Nama, tanggal lahir, dan alamat wajib diisi' });
  }

  try {
    const result = await pool.query(
      `INSERT INTO pasien (name, no_ktp, no_hp, jenis_kelamin, golongan_darah, tanggal_lahir, alamat)
       VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
      [name, no_ktp || null, no_hp || null, jenis_kelamin || null, golongan_darah || null, tanggal_lahir, alamat]
    );
    const patientId = result.rows[0].id.toString();

    console.log(`[gRPC] validateMedicalRecord → patient_id: ${patientId}, name: ${name}`);
    grpcClient.validateMedicalRecord({ patientId, name }, async (err, response) => {
      if (err) {
        console.error('[gRPC] Error:', err.message);
        await pool.query('DELETE FROM pasien WHERE id=$1', [patientId]);
        return res.status(500).json({ success: false, message: 'Gagal validasi ke Service B' });
      }

      console.log(`[gRPC] Response → is_valid: ${response.is_valid}, message: ${response.message}`);

      if (!response.is_valid) {
        await pool.query('DELETE FROM pasien WHERE id=$1', [patientId]);
        console.log(`[gRPC] Pendaftaran ditolak → pasien "${name}" dihapus dari DB`);
        return res.status(409).json({ success: false, message: response.message });
      }

      await publishEvent({ patientId, name, tanggal_lahir, alamat, jenis_kelamin, golongan_darah });
      res.json({ success: true, message: 'Pasien berhasil didaftarkan', patientId });
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, message: err.message });
  }
});

// GET - semua pasien (dengan search)
app.get('/patients', async (req, res) => {
  const { search } = req.query;
  let query = 'SELECT * FROM pasien';
  const params = [];
  if (search) {
    query += ' WHERE name ILIKE $1 OR no_ktp ILIKE $1';
    params.push(`%${search}%`);
  }
  query += ' ORDER BY created_at DESC';
  const result = await pool.query(query, params);
  res.json(result.rows);
});

// GET - detail pasien
app.get('/patients/:id', async (req, res) => {
  const result = await pool.query('SELECT * FROM pasien WHERE id = $1', [req.params.id]);
  if (result.rows.length === 0) return res.status(404).json({ message: 'Pasien tidak ditemukan' });
  res.json(result.rows[0]);
});

// POST - test load (kirim banyak pesan ke RabbitMQ sekaligus)
app.post('/test-load', async (req, res) => {
  const jumlah = parseInt(req.query.n) || 10;
  try {
    const conn = await amqp.connect(process.env.RABBITMQ_URL || 'amqp://localhost');
    const channel = await conn.createChannel();
    await channel.assertQueue('patient.registered');
    for (let i = 1; i <= jumlah; i++) {
      channel.sendToQueue('patient.registered', Buffer.from(JSON.stringify({
        patientId: `TEST-${Date.now()}-${i}`,
        name: `Test Pasien ${i}`,
        tanggal_lahir: '2000-01-01',
        alamat: 'Test Alamat',
      })));
    }
    await channel.close();
    await conn.close();
    res.json({ success: true, message: `${jumlah} pesan dikirim ke RabbitMQ` });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

// PUT - update pasien
app.put('/patients/:id', async (req, res) => {
  const { name, no_ktp, no_hp, jenis_kelamin, golongan_darah, tanggal_lahir, alamat } = req.body;
  if (!name || !tanggal_lahir || !alamat) {
    return res.status(400).json({ success: false, message: 'Nama, tanggal lahir, dan alamat wajib diisi' });
  }
  const result = await pool.query(
    `UPDATE pasien SET name=$1, no_ktp=$2, no_hp=$3, jenis_kelamin=$4, golongan_darah=$5, tanggal_lahir=$6, alamat=$7
     WHERE id=$8 RETURNING *`,
    [name, no_ktp || null, no_hp || null, jenis_kelamin || null, golongan_darah || null, tanggal_lahir, alamat, req.params.id]
  );
  if (result.rows.length === 0) return res.status(404).json({ success: false, message: 'Pasien tidak ditemukan' });
  res.json({ success: true, data: result.rows[0] });
});

// DELETE - hapus pasien
app.delete('/patients/:id', async (req, res) => {
  const pasien = await pool.query('SELECT id FROM pasien WHERE id=$1', [req.params.id]);
  if (pasien.rows.length === 0) return res.status(404).json({ success: false, message: 'Pasien tidak ditemukan' });

  grpcClient.checkPatientRecord({ patientId: req.params.id, name: '' }, async (err, response) => {
    if (err) {
      console.error('gRPC checkPatientRecord error:', err.message);
      return res.status(500).json({ success: false, message: 'Gagal cek rekam medis ke Service B' });
    }

    if (response.has_record) {
      return res.status(409).json({
        success: false,
        message: 'Pasien tidak dapat dihapus karena memiliki rekam medis di Service B',
      });
    }

    const result = await pool.query('DELETE FROM pasien WHERE id=$1 RETURNING id', [req.params.id]);
    res.json({ success: true, message: 'Pasien berhasil dihapus' });
  });
});

// GET - stats
app.get('/stats', async (req, res) => {
  const result = await pool.query('SELECT COUNT(*) as total FROM pasien');
  res.json({ total_pasien: parseInt(result.rows[0].total) });
});

initDB().then(() => {
  app.listen(3001, () => console.log('Service A jalan di port 3001'));
});
