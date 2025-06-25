require('dotenv').config();
const express = require('express');
const multer = require('multer');
const cors = require('cors');
const { TextractClient, AnalyzeDocumentCommand } = require('@aws-sdk/client-textract');
const { S3Client, PutObjectCommand, GetObjectCommand, ListObjectsV2Command, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(cors());
app.use(express.json());

// Multer for handling PDF uploads
const upload = multer({ dest: 'uploads/' });

// AWS clients
const textract = new TextractClient({ region: process.env.AWS_REGION });
const s3 = new S3Client({ region: process.env.AWS_REGION });
const BUCKET = process.env.S3_BUCKET; // must be set in .env

// --- API: Upload PDF, Process with Textract ---
app.post('/upload', upload.single('pdf'), async (req, res) => {
  try {
    const file = req.file;
    const key = uuidv4() + '_' + file.originalname;

    // Upload to S3
    await s3.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: fs.createReadStream(file.path),
      ContentType: file.mimetype,
    }));

    // Analyze document synchronously
    const textractRes = await textract.send(new AnalyzeDocumentCommand({
      Document: { S3Object: { Bucket: BUCKET, Name: key } },
      FeatureTypes: ['FORMS', 'TABLES', 'LAYOUT'],
    }));

    const parsed = parseTextractBlocks(textractRes.Blocks);

    // Clean up temp file
    fs.unlinkSync(file.path);

    res.json(parsed);
  } catch (err) {
    console.error('Upload / Textract error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- List all files in S3 bucket ---
app.get('/files', async (req, res) => {
  try {
    const data = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET }));
    const keys = data.Contents?.map(obj => obj.Key) || [];
    res.json(keys);
  } catch (err) {
    console.error('ListObjects error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- Download PDF file ---
app.get('/files/:name/download', async (req, res) => {
  try {
    const key = req.params.name;
    const getObj = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
    res.setHeader('Content-Type', 'application/pdf');
    getObj.Body.pipe(res);
  } catch (err) {
    console.error('GetObject error:', err);
    res.status(404).json({ error: 'File not found' });
  }
});

// --- Delete PDF file ---
app.delete('/files/:name', async (req, res) => {
  try {
    const key = req.params.name;
    await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: key }));
    res.status(204).end();
  } catch (err) {
    console.error('DeleteObject error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- Analyze existing PDF by name ---
app.get('/files/:name/analysis', async (req, res) => {
  try {
    const key = req.params.name;
    const textractRes = await textract.send(new AnalyzeDocumentCommand({
      Document: { S3Object: { Bucket: BUCKET, Name: key } },
      FeatureTypes: ['FORMS', 'TABLES', 'LAYOUT'],
    }));
    const parsed = parseTextractBlocks(textractRes.Blocks);
    res.json(parsed);
  } catch (err) {
    console.error('Analysis error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- Parsing Logic ---
function parseTextractBlocks(blocks) {
  const blockMap = {};
  const keyMap = {};
  const tableBlocks = [];
  const layoutBlocks = [];
  const pages = {};

  blocks.forEach(b => {
    blockMap[b.Id] = b;
    if (b.BlockType === 'KEY_VALUE_SET') {
      if (b.EntityTypes?.includes('KEY')) keyMap[b.Id] = b;
    }
    if (b.BlockType === 'TABLE') tableBlocks.push(b);
    if (['WORD', 'LINE', 'PAGE'].includes(b.BlockType) || b.BlockType.startsWith('LAYOUT')) {
      layoutBlocks.push(b);
    }
    if (b.BlockType === 'PAGE') {
      pages[b.Page] = { width: b.Geometry.BoundingBox.Width, height: b.Geometry.BoundingBox.Height, lines: [], words: [], page_number: b.Page };
    }
  });

  // Group WORD/LINE by page
  blocks.forEach(b => {
    if ((b.BlockType === 'WORD' || b.BlockType === 'LINE') && b.Page && pages[b.Page]) {
      const item = { text: b.Text, confidence: b.Confidence, boundingBox: b.Geometry.BoundingBox };
      if (b.BlockType === 'WORD') pages[b.Page].words.push(item);
      else pages[b.Page].lines.push(item);
    }
  });

  // Extract key-value pairs
  const fields = [];
  Object.values(keyMap).forEach(keyBlock => {
    const keyText = [];
    keyBlock.Relationships?.forEach(r => {
      if (r.Type === 'CHILD') r.Ids.forEach(id => { const w = blockMap[id]; if (w.BlockType === 'WORD') keyText.push(w.Text); });
    });
    const valRel = keyBlock.Relationships?.find(r => r.Type === 'VALUE');
    if (!valRel) return;
    const valBlock = blockMap[valRel.Ids[0]];
    const valText = [];
    valBlock.Relationships?.forEach(r => {
      if (r.Type === 'CHILD') r.Ids.forEach(id => { const w = blockMap[id]; if (w.BlockType === 'WORD') valText.push(w.Text); });
    });
    fields.push({ key: keyText.join(' '), value: valText.join(' '), key_bbox: keyBlock.Geometry.BoundingBox, value_bbox: valBlock.Geometry.BoundingBox, confidence: { key: keyBlock.Confidence, value: valBlock.Confidence } });
  });

  // Extract tables and cells
  const tables = tableBlocks.map(tb => {
    const cells = [];
    tb.Relationships?.forEach(rel => {
      if (rel.Type === 'CHILD') rel.Ids.forEach(cellId => {
        const cell = blockMap[cellId];
        if (cell.BlockType === 'CELL') {
          const txt = [];
          cell.Relationships?.forEach(r2 => {
            if (r2.Type === 'CHILD') r2.Ids.forEach(wid => { const w = blockMap[wid]; if (w.BlockType === 'WORD') txt.push(w.Text); });
          });
          cells.push({ row: cell.RowIndex, column: cell.ColumnIndex, text: txt.join(' '), boundingBox: cell.Geometry.BoundingBox, confidence: cell.Confidence });
        }
      });
    });
    return { page: tb.Page, rows_count: tb.RowCount, columns_count: tb.ColumnCount, cells };
  });

  // Layout blocks
  const layout = layoutBlocks.map(b => ({ type: b.BlockType, text: (b.Text || getTextForBlock(b, blockMap)).trim(), bbox: b.Geometry?.BoundingBox, confidence: b.Confidence }));

  return { fields, tables, pages: Object.values(pages), layout };
}

// Helper to extract text from relationships
function getTextForBlock(block, blockMap) {
  if (!block.Relationships) return '';
  const txt = [];
  block.Relationships.forEach(rel => {
    if (rel.Type === 'CHILD') rel.Ids.forEach(id => {
      const c = blockMap[id];
      if (c.BlockType === 'WORD') txt.push(c.Text);
      if (c.BlockType === 'SELECTION_ELEMENT') txt.push(c.SelectionStatus === 'SELECTED' ? '[X]' : '[ ]');
    });
  });
  return txt.join(' ');
}

// Start server
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));
