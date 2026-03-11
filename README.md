# โค้ดจริงที่ใช้ในระบบ (คัดจาก `tx.service.js`)

## Bee-inspired ordering (จริง)

```js
function beeInspiredUnitOrdering(units, capacity, iterations = 120) {
  // กันกรณีไม่มีข้อมูล
  if (!Array.isArray(units) || units.length === 0) return [];

  // เริ่มจากเรียงปริมาตรหน่วยจากมาก -> น้อย (best-first base)
  const base = units
    .slice()
    .sort((a, b) => Number(b.unit_volume_m3 || 0) - Number(a.unit_volume_m3 || 0));

  // bestOrder/bestScore คือคำตอบที่ดีที่สุดที่พบระหว่างวนหา
  let bestOrder = base.slice();
  let bestScore = packUnitsFirstFit(bestOrder, capacity).length;
  const n = base.length;

  if (n <= 1) return bestOrder;

  // local search แบบ deterministic: สลับตำแหน่งแล้ววัดจำนวนกล่องใหม่
  for (let k = 0; k < iterations; k += 1) {
    const candidate = bestOrder.slice();
    const i = k % n;
    const j = (k * 7 + 3) % n;
    if (i === j) continue;
    const tmp = candidate[i];
    candidate[i] = candidate[j];
    candidate[j] = tmp;

    // score = จำนวนกล่องที่ต้องใช้ (ยิ่งน้อยยิ่งดี)
    const score = packUnitsFirstFit(candidate, capacity).length;

    // รับลำดับใหม่เมื่อดีกว่า หรือเท่ากับของเดิม
    if (score <= bestScore) {
      bestScore = score;
      bestOrder = candidate;
    }
  }
  return bestOrder;
}
```

## ตัวผสมหลักของระบบ (จริง)

```js
function buildPackingFromResolvedItems(items, boxM3, options = {}) {
  // จำกัดจำนวน unit สูงสุดเพื่อลดเวลา solver
  const maxUnitsForSolver = Number(options.maxUnitsForSolver || 2500);

  // แปลง items -> units ที่มี unit_volume_m3 ใช้แพ็กแบบปริมาตร
  const packingUnitsMeta = buildPackingUnits(items, maxUnitsForSolver);

  // 1) จัดลำดับด้วย bee-inspired
  const beeOrderUnits = beeInspiredUnitOrdering(packingUnitsMeta.units, boxM3, 120);

  // 2) แพ็กด้วย first-fit ตามลำดับที่ได้
  const packedBoxes = packUnitsFirstFit(beeOrderUnits, boxM3);
  const packedSummary = summarizeBoxes(packedBoxes, boxM3);

  // ค่าสถิติรวมเพื่อเทียบความสมเหตุสมผลของจำนวนกล่อง
  const totalQty = (items || []).reduce((s, x) => s + Number(x.qty || 0), 0);
  const totalM3 = (items || []).reduce((s, x) => s + Number(x.qty || 0) * Number(x.unit_volume_m3 || 0), 0);
  const unitVolumes = packingUnitsMeta.units.map((u) => Number(u.unit_volume_m3 || 0));
  const theoreticalMin = Math.ceil(totalM3 / boxM3);
  const greedyBoxes = unitVolumes.length > 0 ? firstFitBinCount(unitVolumes.slice().sort((a, b) => b - a), boxM3) : theoreticalMin;
  const beeBoxesRaw = packedSummary.summary.total_boxes > 0 ? packedSummary.summary.total_boxes : theoreticalMin;

  // ข้อเสนอจากโมเดลปริมาตร: ไม่ต่ำกว่า theoreticalMin และไม่สูงเกินจำเป็น
  const suggestedBoxes = Math.max(theoreticalMin, Math.min(greedyBoxes, beeBoxesRaw));

  const notes = [];
  if (packingUnitsMeta.invalid_volume_units > 0) {
    notes.push(`Ignored ${packingUnitsMeta.invalid_volume_units} unit(s) with invalid volume during packing details.`);
  }
  if (packingUnitsMeta.capped_unit_count > 0) {
    notes.push(`Packing detail approximated: ${packingUnitsMeta.capped_unit_count} unit(s) omitted by solver cap ${maxUnitsForSolver}.`);
  }

  // 3) คำนวณ real 2D layout จากมิติจริง (กว้างxยาวxสูง)
  const layout2D = build2DLayoutFromResolvedItems(items, { maxUnitsForSolver });
  notes.push(...layout2D.notes);
  const real2DBoxes = Number(layout2D?.layout?.summary?.total_boxes || 0);

  // ถ้ามีผล 2D ใช้ผล 2D เป็นค่าจริงสุดท้าย
  const suggestedBoxesFinal = real2DBoxes > 0 ? real2DBoxes : suggestedBoxes;
  if (real2DBoxes > 0 && real2DBoxes !== suggestedBoxes) {
    notes.push(`Suggested boxes now follow Real 2D layout: ${real2DBoxes} (volume model was ${suggestedBoxes}).`);
  }

  return {
    totals: {
      box_m3: boxM3,
      total_qty: totalQty,
      total_m3: totalM3,
      theoretical_min_boxes: theoreticalMin,
      greedy_boxes: greedyBoxes,
      bee_boxes: beeBoxesRaw,
      volume_suggested_boxes: suggestedBoxes,
      real_2d_boxes: real2DBoxes,
      suggested_boxes: suggestedBoxesFinal,
    },
    packing: {
      algorithm: "bee+first_fit",
      boxes: packedSummary.boxes,
      summary: packedSummary.summary,
      layout_2d: layout2D.layout,
    },
    notes,
  };
}
```

แหล่งโค้ด: `backend/src/services/tx.service.js`

## คำอธิบายเพิ่มเติม (สั้น)

- `beeInspiredUnitOrdering` ทำหน้าที่ "จัดลำดับหน่วยสินค้า" ก่อนแพ็ก เพื่อให้ `packUnitsFirstFit` มีโอกาสใช้กล่องน้อยลง
- ตัวแปร `bestScore` คือจำนวนกล่องที่ใช้ได้จากลำดับปัจจุบัน และจะอัปเดตเมื่อเจอลำดับที่ดีกว่าหรือเท่ากัน
- ใน `buildPackingFromResolvedItems` มีการคำนวณ 3 ค่าเทียบกัน: `theoretical_min_boxes`, `greedy_boxes`, `bee_boxes`
- ค่ากลางจากโมเดลปริมาตรคือ `volume_suggested_boxes`
- จากนั้นระบบจะคำนวณ `layout_2d` เพิ่ม และถ้ามี `real_2d_boxes > 0` จะใช้ค่านี้เป็น `suggested_boxes` ทันที เพราะสะท้อนการจัดวางจริงมากกว่า
