const pool = require("../db/pool");
const DEFAULT_BOX_M3 = 0.036;
const OLLAMA_URL = process.env.OLLAMA_URL || "http://127.0.0.1:11434";
const OLLAMA_MODEL = process.env.OLLAMA_MODEL || "qwen2.5:1.5b";
const LAYOUT_BOX_BASE_CM = { width: 40, length: 60, height: 15 };

function toSafeLimit(x, fallback = 50) {
  const n = Number(x);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(Math.floor(n), 200);
}

function clampInt(x, min, max) {
  const n = Number(x);
  if (!Number.isFinite(n)) return min;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

function firstFitBinCount(volumes, capacity) {
  const bins = [];
  for (const v of volumes) {
    let placed = false;
    for (let i = 0; i < bins.length; i += 1) {
      if (bins[i] + v <= capacity + 1e-12) {
        bins[i] += v;
        placed = true;
        break;
      }
    }
    if (!placed) bins.push(v);
  }
  return bins.length;
}

function toPositiveInt(x) {
  const n = Number(x);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.floor(n));
}

function toPositiveNumber(x) {
  const n = Number(x);
  if (!Number.isFinite(n)) return 0;
  return n > 0 ? n : 0;
}

function validateEditableLotAvailability(requestedByLot, oldReservedByLot, lotMap) {
  for (const [lotId, needQtyRaw] of requestedByLot.entries()) {
    const needQty = Number(needQtyRaw || 0);
    const lot = lotMap.get(Number(lotId));
    if (!lot) throw new Error(`lot_id not found: ${lotId}`);
    const currentRemaining = Number(lot.remaining_qty || 0);
    const alreadyReserved = Number(oldReservedByLot.get(Number(lotId)) || 0);
    const editableAvailable = currentRemaining + alreadyReserved;
    if (editableAvailable < needQty) {
      throw new Error(
        `insufficient lot stock: lot_id ${lotId}, requested ${needQty}, available ${editableAvailable} (remaining ${currentRemaining}, reserved_in_tx ${alreadyReserved})`
      );
    }
  }
}

function buildLotDeltaByLot(requestedByLot, oldReservedByLot) {
  const lotIds = Array.from(new Set([
    ...Array.from(requestedByLot.keys()).map((x) => Number(x)),
    ...Array.from(oldReservedByLot.keys()).map((x) => Number(x)),
  ])).filter((x) => Number.isInteger(x) && x > 0);

  const deltaByLot = new Map();
  for (const lotId of lotIds) {
    const newQty = Number(requestedByLot.get(Number(lotId)) || 0);
    const oldQty = Number(oldReservedByLot.get(Number(lotId)) || 0);
    deltaByLot.set(Number(lotId), Number(newQty) - Number(oldQty));
  }
  return deltaByLot;
}

function buildPackingUnits(items, maxUnits) {
  let validUnitCount = 0;
  let invalidVolumeUnits = 0;
  for (const item of items || []) {
    const qty = toPositiveInt(item?.qty);
    const unitVol = Number(item?.unit_volume_m3 || 0);
    if (qty <= 0) continue;
    if (!(unitVol > 0)) {
      invalidVolumeUnits += qty;
      continue;
    }
    validUnitCount += qty;
  }

  const targetUnitCount = Math.min(validUnitCount, maxUnits);
  const cappedUnitCount = Math.max(0, validUnitCount - targetUnitCount);
  const units = [];
  let left = targetUnitCount;

  for (const item of items || []) {
    if (left <= 0) break;
    const qty = toPositiveInt(item?.qty);
    const unitVol = Number(item?.unit_volume_m3 || 0);
    if (qty <= 0 || !(unitVol > 0)) continue;
    const take = Math.min(qty, left);
    for (let i = 0; i < take; i += 1) {
      units.push({
        product_id: Number(item.product_id),
        product_name: item.product_name || `Product #${item.product_id}`,
        unit_volume_m3: unitVol,
      });
    }
    left -= take;
  }

  return {
    units,
    valid_unit_count: validUnitCount,
    invalid_volume_units: invalidVolumeUnits,
    capped_unit_count: cappedUnitCount,
  };
}

function packUnitsFirstFit(units, capacity) {
  const boxes = [];
  for (const unit of units || []) {
    const v = Number(unit?.unit_volume_m3 || 0);
    if (!(v > 0)) continue;
    let idx = -1;
    for (let i = 0; i < boxes.length; i += 1) {
      if (boxes[i].used_m3 + v <= capacity + 1e-12) {
        idx = i;
        break;
      }
    }
    if (idx === -1) {
      boxes.push({ used_m3: 0, products: new Map() });
      idx = boxes.length - 1;
    }
    const box = boxes[idx];
    box.used_m3 += v;
    const key = Number(unit.product_id);
    const prev = box.products.get(key) || {
      product_id: Number(unit.product_id),
      product_name: unit.product_name,
      qty: 0,
    };
    prev.qty += 1;
    box.products.set(key, prev);
  }
  return boxes;
}

function beeInspiredUnitOrdering(units, capacity, iterations = 120) {
  if (!Array.isArray(units) || units.length === 0) return [];
  const base = units
    .slice()
    .sort((a, b) => Number(b.unit_volume_m3 || 0) - Number(a.unit_volume_m3 || 0));
  let bestOrder = base.slice();
  let bestScore = packUnitsFirstFit(bestOrder, capacity).length;
  const n = base.length;

  if (n <= 1) return bestOrder;

  // Deterministic local search around a best-first base order.
  for (let k = 0; k < iterations; k += 1) {
    const candidate = bestOrder.slice();
    const i = k % n;
    const j = (k * 7 + 3) % n;
    if (i === j) continue;
    const tmp = candidate[i];
    candidate[i] = candidate[j];
    candidate[j] = tmp;
    const score = packUnitsFirstFit(candidate, capacity).length;
    if (score <= bestScore) {
      bestScore = score;
      bestOrder = candidate;
    }
  }
  return bestOrder;
}

function summarizeBoxes(boxes, boxM3) {
  const rows = (boxes || []).map((box, idx) => {
    const used = Number(box.used_m3 || 0);
    const utilization = boxM3 > 0 ? Math.min(100, (used / boxM3) * 100) : 0;
    const empty = Math.max(0, 100 - utilization);
    const products = Array.from(box.products.values())
      .sort((a, b) => String(a.product_name || "").localeCompare(String(b.product_name || "")))
      .map((p) => ({
        product_id: Number(p.product_id),
        product_name: p.product_name,
        qty: Number(p.qty || 0),
      }));

    return {
      box_no: idx + 1,
      used_m3: Number(used.toFixed(6)),
      utilization_pct: Number(utilization.toFixed(1)),
      empty_pct: Number(empty.toFixed(1)),
      products,
    };
  });

  const total = rows.length;
  const utils = rows.map((r) => Number(r.utilization_pct || 0));
  const avg = total > 0 ? utils.reduce((s, x) => s + x, 0) / total : 0;

  return {
    boxes: rows,
    summary: {
      total_boxes: total,
      avg_utilization_pct: Number(avg.toFixed(1)),
      min_utilization_pct: Number((total > 0 ? Math.min(...utils) : 0).toFixed(1)),
      max_utilization_pct: Number((total > 0 ? Math.max(...utils) : 0).toFixed(1)),
    },
  };
}

function removeContainedRects(rects) {
  const eps = 1e-9;
  return rects.filter((a, i) => !rects.some((b, j) => {
    if (i === j) return false;
    return (
      a.x_cm + eps >= b.x_cm &&
      a.y_cm + eps >= b.y_cm &&
      a.x_cm + a.w_cm <= b.x_cm + b.w_cm + eps &&
      a.y_cm + a.l_cm <= b.y_cm + b.l_cm + eps
    );
  }));
}

function build2DLayoutUnits(items, maxUnits) {
  const baseW = Number(LAYOUT_BOX_BASE_CM.width || 0);
  const baseL = Number(LAYOUT_BOX_BASE_CM.length || 0);
  const baseH = Number(LAYOUT_BOX_BASE_CM.height || 0);

  let validUnitCount = 0;
  let invalidDimensionUnits = 0;
  let tooTallUnits = 0;
  let tooLargeFootprintUnits = 0;

  for (const item of items || []) {
    const qty = toPositiveInt(item?.qty);
    const w = toPositiveNumber(item?.unit_width_cm);
    const l = toPositiveNumber(item?.unit_length_cm);
    const h = toPositiveNumber(item?.unit_height_cm);
    if (qty <= 0) continue;
    if (!(w > 0 && l > 0 && h > 0)) {
      invalidDimensionUnits += qty;
      continue;
    }
    if (h > baseH + 1e-9) {
      tooTallUnits += qty;
      continue;
    }
    const fits = (w <= baseW + 1e-9 && l <= baseL + 1e-9) || (l <= baseW + 1e-9 && w <= baseL + 1e-9);
    if (!fits) {
      tooLargeFootprintUnits += qty;
      continue;
    }
    validUnitCount += qty;
  }

  const targetUnitCount = Math.min(validUnitCount, maxUnits);
  const cappedUnitCount = Math.max(0, validUnitCount - targetUnitCount);
  const units = [];
  let left = targetUnitCount;

  for (const item of items || []) {
    if (left <= 0) break;
    const qty = toPositiveInt(item?.qty);
    const w = toPositiveNumber(item?.unit_width_cm);
    const l = toPositiveNumber(item?.unit_length_cm);
    const h = toPositiveNumber(item?.unit_height_cm);
    if (qty <= 0 || !(w > 0 && l > 0 && h > 0)) continue;
    if (h > baseH + 1e-9) continue;
    const fits = (w <= baseW + 1e-9 && l <= baseL + 1e-9) || (l <= baseW + 1e-9 && w <= baseL + 1e-9);
    if (!fits) continue;
    const take = Math.min(qty, left);
    for (let i = 0; i < take; i += 1) {
      units.push({
        product_id: Number(item.product_id),
        product_name: item.product_name || `Product #${item.product_id}`,
        width_cm: w,
        length_cm: l,
        height_cm: h,
        area_cm2: w * l,
      });
    }
    left -= take;
  }

  return {
    units,
    valid_unit_count: validUnitCount,
    invalid_dimension_units: invalidDimensionUnits,
    too_tall_units: tooTallUnits,
    too_large_footprint_units: tooLargeFootprintUnits,
    capped_unit_count: cappedUnitCount,
  };
}

function tryPlaceIn2DBox(box, unit) {
  const eps = 1e-9;
  const orientations = [
    { w_cm: Number(unit.width_cm || 0), l_cm: Number(unit.length_cm || 0), rotated: false },
  ];
  if (Math.abs(Number(unit.width_cm || 0) - Number(unit.length_cm || 0)) > eps) {
    orientations.push({
      w_cm: Number(unit.length_cm || 0),
      l_cm: Number(unit.width_cm || 0),
      rotated: true,
    });
  }

  for (let i = 0; i < box.free_rects.length; i += 1) {
    const rect = box.free_rects[i];
    for (const orient of orientations) {
      if (!(orient.w_cm > 0 && orient.l_cm > 0)) continue;
      if (orient.w_cm <= rect.w_cm + eps && orient.l_cm <= rect.l_cm + eps) {
        const placement = {
          x_cm: Number(rect.x_cm.toFixed(2)),
          y_cm: Number(rect.y_cm.toFixed(2)),
          w_cm: Number(orient.w_cm.toFixed(2)),
          l_cm: Number(orient.l_cm.toFixed(2)),
          rotated: !!orient.rotated,
        };
        box.used_area_cm2 += orient.w_cm * orient.l_cm;
        const key = Number(unit.product_id);
        const prev = box.products.get(key) || {
          product_id: Number(unit.product_id),
          product_name: unit.product_name,
          height_cm: Number(unit.height_cm || 0),
          qty: 0,
          placements: [],
        };
        prev.qty += 1;
        prev.placements.push(placement);
        box.products.set(key, prev);

        const rightW = rect.w_cm - orient.w_cm;
        const topL = rect.l_cm - orient.l_cm;
        const nextRects = box.free_rects.slice();
        nextRects.splice(i, 1);
        if (rightW > eps) {
          nextRects.push({
            x_cm: rect.x_cm + orient.w_cm,
            y_cm: rect.y_cm,
            w_cm: rightW,
            l_cm: orient.l_cm,
          });
        }
        if (topL > eps) {
          nextRects.push({
            x_cm: rect.x_cm,
            y_cm: rect.y_cm + orient.l_cm,
            w_cm: rect.w_cm,
            l_cm: topL,
          });
        }
        box.free_rects = removeContainedRects(
          nextRects.filter((r) => r.w_cm > eps && r.l_cm > eps)
        );
        return true;
      }
    }
  }
  return false;
}

function applyPlacement2D(box, unit, rectIndex, rect, orient) {
  const eps = 1e-9;
  const placement = {
    x_cm: Number(rect.x_cm.toFixed(2)),
    y_cm: Number(rect.y_cm.toFixed(2)),
    w_cm: Number(orient.w_cm.toFixed(2)),
    l_cm: Number(orient.l_cm.toFixed(2)),
    rotated: !!orient.rotated,
  };
  box.used_area_cm2 += orient.w_cm * orient.l_cm;
  const key = Number(unit.product_id);
  const prev = box.products.get(key) || {
    product_id: Number(unit.product_id),
    product_name: unit.product_name,
    height_cm: Number(unit.height_cm || 0),
    qty: 0,
    placements: [],
  };
  prev.qty += 1;
  prev.placements.push(placement);
  box.products.set(key, prev);

  const rightW = rect.w_cm - orient.w_cm;
  const topL = rect.l_cm - orient.l_cm;
  const nextRects = box.free_rects.slice();
  nextRects.splice(rectIndex, 1);
  if (rightW > eps) {
    nextRects.push({
      x_cm: rect.x_cm + orient.w_cm,
      y_cm: rect.y_cm,
      w_cm: rightW,
      l_cm: orient.l_cm,
    });
  }
  if (topL > eps) {
    nextRects.push({
      x_cm: rect.x_cm,
      y_cm: rect.y_cm + orient.l_cm,
      w_cm: rect.w_cm,
      l_cm: topL,
    });
  }
  box.free_rects = removeContainedRects(nextRects.filter((r) => r.w_cm > eps && r.l_cm > eps));
}

function tryPlaceIn2DBoxBestFit(box, unit) {
  const eps = 1e-9;
  const orientations = [
    { w_cm: Number(unit.width_cm || 0), l_cm: Number(unit.length_cm || 0), rotated: false },
  ];
  if (Math.abs(Number(unit.width_cm || 0) - Number(unit.length_cm || 0)) > eps) {
    orientations.push({
      w_cm: Number(unit.length_cm || 0),
      l_cm: Number(unit.width_cm || 0),
      rotated: true,
    });
  }

  function rectGapDistance(a, b) {
    const ax2 = Number(a.x_cm) + Number(a.w_cm);
    const ay2 = Number(a.y_cm) + Number(a.l_cm);
    const bx2 = Number(b.x_cm) + Number(b.w_cm);
    const by2 = Number(b.y_cm) + Number(b.l_cm);
    const dx = Math.max(0, Math.max(Number(b.x_cm) - ax2, Number(a.x_cm) - bx2));
    const dy = Math.max(0, Math.max(Number(b.y_cm) - ay2, Number(a.y_cm) - by2));
    return dx + dy;
  }

  let best = null;
  for (let i = 0; i < box.free_rects.length; i += 1) {
    const rect = box.free_rects[i];
    for (const orient of orientations) {
      if (!(orient.w_cm > 0 && orient.l_cm > 0)) continue;
      if (!(orient.w_cm <= rect.w_cm + eps && orient.l_cm <= rect.l_cm + eps)) continue;
      const waste = Math.max(0, rect.w_cm * rect.l_cm - orient.w_cm * orient.l_cm);
      const shortSide = Math.min(
        Math.max(0, rect.w_cm - orient.w_cm),
        Math.max(0, rect.l_cm - orient.l_cm)
      );
      const candidateRect = {
        x_cm: rect.x_cm,
        y_cm: rect.y_cm,
        w_cm: orient.w_cm,
        l_cm: orient.l_cm,
      };
      const prevProduct = box.products.get(Number(unit.product_id));
      let clusterPenalty = 0;
      if (prevProduct && Array.isArray(prevProduct.placements) && prevProduct.placements.length > 0) {
        const minX = Math.min(...prevProduct.placements.map((p) => Number(p.x_cm || 0)));
        const minY = Math.min(...prevProduct.placements.map((p) => Number(p.y_cm || 0)));
        const maxX = Math.max(...prevProduct.placements.map((p) => Number(p.x_cm || 0) + Number(p.w_cm || 0)));
        const maxY = Math.max(...prevProduct.placements.map((p) => Number(p.y_cm || 0) + Number(p.l_cm || 0)));
        clusterPenalty = rectGapDistance(candidateRect, {
          x_cm: minX,
          y_cm: minY,
          w_cm: Math.max(0, maxX - minX),
          l_cm: Math.max(0, maxY - minY),
        });
      } else {
        // No fixed side preference: let best-fit + remaining tie-breakers decide placement naturally.
        clusterPenalty = 0;
      }

      const score = prevProduct
        ? [clusterPenalty, waste, shortSide, rect.y_cm, rect.x_cm]
        : [waste, shortSide, clusterPenalty, rect.y_cm, rect.x_cm];
      if (!best || score[0] < best.score[0] - eps ||
        (Math.abs(score[0] - best.score[0]) <= eps && (
          score[1] < best.score[1] - eps ||
          (Math.abs(score[1] - best.score[1]) <= eps && (
            score[2] < best.score[2] - eps ||
            (Math.abs(score[2] - best.score[2]) <= eps && score[3] < best.score[3] - eps)
          ))
        ))) {
        best = { rectIndex: i, rect, orient, score };
      }
    }
  }

  if (!best) return false;
  applyPlacement2D(box, unit, best.rectIndex, best.rect, best.orient);
  return true;
}

function packUnits2D(units, placer) {
  const baseW = Number(LAYOUT_BOX_BASE_CM.width || 0);
  const baseL = Number(LAYOUT_BOX_BASE_CM.length || 0);
  const ordered = (units || [])
    .slice()
    .sort((a, b) => {
      const areaDiff = Number(b.area_cm2 || 0) - Number(a.area_cm2 || 0);
      if (areaDiff !== 0) return areaDiff;
      return Number(a.product_id || 0) - Number(b.product_id || 0);
    });

  const boxes = [];
  let unplacedCount = 0;
  for (const unit of ordered) {
    let placed = false;
    for (const box of boxes) {
      if (placer(box, unit)) {
        placed = true;
        break;
      }
    }
    if (!placed) {
      const box = {
        used_area_cm2: 0,
        free_rects: [{ x_cm: 0, y_cm: 0, w_cm: baseW, l_cm: baseL }],
        products: new Map(),
      };
      if (placer(box, unit)) {
        boxes.push(box);
      } else {
        unplacedCount += 1;
      }
    }
  }
  return { boxes, unplaced_count: unplacedCount };
}

function packUnitsFirstFit2D(units) {
  return packUnits2D(units, tryPlaceIn2DBox);
}

function packUnitsBestFit2D(units) {
  return packUnits2D(units, tryPlaceIn2DBoxBestFit);
}

function summarize2DBoxes(boxes, algorithm = "first_fit_decreasing_2d_rotate90") {
  const boxArea = Number(LAYOUT_BOX_BASE_CM.width || 0) * Number(LAYOUT_BOX_BASE_CM.length || 0);
  const rows = (boxes || []).map((box, idx) => {
    const usedArea = Number(box.used_area_cm2 || 0);
    const usedPct = boxArea > 0 ? Math.min(100, (usedArea / boxArea) * 100) : 0;
    const emptyPct = Math.max(0, 100 - usedPct);
    const items = Array.from(box.products.values())
      .sort((a, b) => String(a.product_name || "").localeCompare(String(b.product_name || "")))
      .map((p) => ({
        product_id: Number(p.product_id),
        product_name: p.product_name,
        height_cm: Number(p.height_cm || 0),
        qty: Number(p.qty || 0),
        placements: (p.placements || []).map((pl) => ({
          x_cm: Number(pl.x_cm),
          y_cm: Number(pl.y_cm),
          w_cm: Number(pl.w_cm),
          l_cm: Number(pl.l_cm),
          rotated: !!pl.rotated,
        })),
      }));
    return {
      box_no: idx + 1,
      used_area_pct: Number(usedPct.toFixed(1)),
      empty_area_pct: Number(emptyPct.toFixed(1)),
      items,
      notes: [],
    };
  });

  const total = rows.length;
  const usedArr = rows.map((r) => Number(r.used_area_pct || 0));
  const avgUsed = total > 0 ? usedArr.reduce((s, x) => s + x, 0) / total : 0;
  const avgEmpty = total > 0 ? rows.reduce((s, r) => s + Number(r.empty_area_pct || 0), 0) / total : 0;

  return {
    box_base_cm: { ...LAYOUT_BOX_BASE_CM },
    algorithm,
    boxes: rows,
    summary: {
      total_boxes: total,
      avg_used_area_pct: Number(avgUsed.toFixed(1)),
      avg_empty_area_pct: Number(avgEmpty.toFixed(1)),
    },
    notes: [],
  };
}

function score2DPacking(packed) {
  const baseArea = Number(LAYOUT_BOX_BASE_CM.width || 0) * Number(LAYOUT_BOX_BASE_CM.length || 0);
  const boxes = packed?.boxes || [];
  const unplaced = Number(packed?.unplaced_count || 0);
  const boxCount = boxes.length;
  const freeRectCount = boxes.reduce((sum, b) => sum + Number((b.free_rects || []).length || 0), 0);
  const emptyArea = boxes.reduce((sum, b) => {
    const used = Number(b.used_area_cm2 || 0);
    return sum + Math.max(0, baseArea - used);
  }, 0);
  return { unplaced, boxCount, freeRectCount, emptyArea };
}

function isPackingScoreBetter(a, b) {
  if (!a) return true;
  if (b.unplaced !== a.unplaced) return b.unplaced < a.unplaced;
  if (b.boxCount !== a.boxCount) return b.boxCount < a.boxCount;
  if (b.freeRectCount !== a.freeRectCount) return b.freeRectCount < a.freeRectCount;
  return b.emptyArea < a.emptyArea - 1e-9;
}

function build2DLayoutFromResolvedItems(items, options = {}) {
  const maxUnitsForSolver = Number(options.maxUnitsForSolver || 2500);
  const hardLimitRaw = Number(options.maxBoxesHardLimit ?? process.env.AI_ARRANGE_HARDLIMIT_BOXES ?? 12);
  const hardLimitBoxes = Number.isFinite(hardLimitRaw) && hardLimitRaw > 0 ? Math.floor(hardLimitRaw) : 12;
  const meta = build2DLayoutUnits(items, maxUnitsForSolver);
  const packedFirstFit = packUnitsFirstFit2D(meta.units);
  const packedBestFit = packUnitsBestFit2D(meta.units);
  const scoreFirstFit = score2DPacking(packedFirstFit);
  const scoreBestFit = score2DPacking(packedBestFit);

  const useBestFit = isPackingScoreBetter(scoreFirstFit, scoreBestFit);
  const packed = useBestFit ? packedBestFit : packedFirstFit;
  const raw2DBoxes = Array.isArray(packed?.boxes) ? packed.boxes : [];
  const limited2DBoxes = raw2DBoxes.slice(0, hardLimitBoxes);
  const layout = summarize2DBoxes(
    limited2DBoxes,
    useBestFit ? "best_fit_decreasing_2d_rotate90_auto" : "first_fit_decreasing_2d_rotate90_auto"
  );
  const notes = [];
  if (meta.invalid_dimension_units > 0) {
    notes.push(`Ignored ${meta.invalid_dimension_units} unit(s) missing real dimensions.`);
  }
  if (meta.too_tall_units > 0) {
    notes.push(`Ignored ${meta.too_tall_units} unit(s): height exceeds ${LAYOUT_BOX_BASE_CM.height} cm.`);
  }
  if (meta.too_large_footprint_units > 0) {
    notes.push(`Ignored ${meta.too_large_footprint_units} unit(s): footprint cannot fit ${LAYOUT_BOX_BASE_CM.width}x${LAYOUT_BOX_BASE_CM.length} cm.`);
  }
  if (meta.capped_unit_count > 0) {
    notes.push(`2D layout approximated: ${meta.capped_unit_count} unit(s) omitted by solver cap ${maxUnitsForSolver}.`);
  }
  if (packed.unplaced_count > 0) {
    notes.push(`2D layout could not place ${packed.unplaced_count} unit(s) due to geometric constraints.`);
  }
  if (raw2DBoxes.length > hardLimitBoxes) {
    notes.push(`2D layout preview capped by hard limit: ${hardLimitBoxes} boxes.`);
  }
  layout.notes = notes;
  return { layout, notes };
}

function rebalanceCandidateQuantities(products, targetTotalQty, availableByProduct) {
  const rows = (products || []).map((p) => ({
    product_id: Number(p.product_id),
    product_name: p.product_name,
    target_qty: Math.max(1, toPositiveInt(p.target_qty)),
  }));
  if (rows.length === 0) return rows;

  const currentTotal = rows.reduce((s, r) => s + r.target_qty, 0);
  if (currentTotal <= 0 || !Number.isFinite(Number(targetTotalQty)) || Number(targetTotalQty) <= 0) {
    return rows;
  }

  const desired = Math.max(rows.length, Math.round(Number(targetTotalQty)));
  const factor = desired / currentTotal;
  const scaled = rows.map((r) => {
    const cap = Math.max(1, toPositiveInt(availableByProduct.get(r.product_id) || 0));
    const scaledQty = Math.max(1, Math.round(r.target_qty * factor));
    return { ...r, target_qty: Math.min(scaledQty, cap), cap };
  });

  let sum = scaled.reduce((s, r) => s + r.target_qty, 0);
  if (sum < desired) {
    const growable = scaled
      .filter((r) => r.target_qty < r.cap)
      .sort((a, b) => b.cap - a.cap);
    let i = 0;
    while (sum < desired && growable.length > 0) {
      const row = growable[i % growable.length];
      if (row.target_qty < row.cap) {
        row.target_qty += 1;
        sum += 1;
      }
      i += 1;
      if (i > desired * 4) break;
    }
  } else if (sum > desired) {
    const shrinkable = scaled
      .filter((r) => r.target_qty > 1)
      .sort((a, b) => b.target_qty - a.target_qty);
    let i = 0;
    while (sum > desired && shrinkable.length > 0) {
      const row = shrinkable[i % shrinkable.length];
      if (row.target_qty > 1) {
        row.target_qty -= 1;
        sum -= 1;
      }
      i += 1;
      if (i > desired * 4) break;
    }
  }

  return scaled.map(({ cap, ...rest }) => rest);
}

function buildPackingFromResolvedItems(items, boxM3, options = {}) {
  const maxUnitsForSolver = Number(options.maxUnitsForSolver || 2500);
  const hardLimitRaw = Number(options.maxBoxesHardLimit ?? process.env.AI_ARRANGE_HARDLIMIT_BOXES ?? 12);
  const hardLimitBoxes = Number.isFinite(hardLimitRaw) && hardLimitRaw > 0 ? Math.floor(hardLimitRaw) : 12;
  const packingUnitsMeta = buildPackingUnits(items, maxUnitsForSolver);
  const beeOrderUnits = beeInspiredUnitOrdering(packingUnitsMeta.units, boxM3, 120);
  const packedBoxes = packUnitsFirstFit(beeOrderUnits, boxM3);
  const packedSummary = summarizeBoxes(packedBoxes, boxM3);

  const totalQty = (items || []).reduce((s, x) => s + Number(x.qty || 0), 0);
  const totalM3 = (items || []).reduce((s, x) => s + Number(x.qty || 0) * Number(x.unit_volume_m3 || 0), 0);
  const unitVolumes = packingUnitsMeta.units.map((u) => Number(u.unit_volume_m3 || 0));
  const theoreticalMin = Math.ceil(totalM3 / boxM3);
  const greedyBoxes = unitVolumes.length > 0 ? firstFitBinCount(unitVolumes.slice().sort((a, b) => b - a), boxM3) : theoreticalMin;
  const beeBoxesRaw = packedSummary.summary.total_boxes > 0 ? packedSummary.summary.total_boxes : theoreticalMin;
  const suggestedBoxes = Math.max(theoreticalMin, Math.min(greedyBoxes, beeBoxesRaw));

  const notes = [];
  if (packingUnitsMeta.invalid_volume_units > 0) {
    notes.push(`Ignored ${packingUnitsMeta.invalid_volume_units} unit(s) with invalid volume during packing details.`);
  }
  if (packingUnitsMeta.capped_unit_count > 0) {
    notes.push(`Packing detail approximated: ${packingUnitsMeta.capped_unit_count} unit(s) omitted by solver cap ${maxUnitsForSolver}.`);
  }
  const layout2D = build2DLayoutFromResolvedItems(items, { maxUnitsForSolver, maxBoxesHardLimit: hardLimitBoxes });
  notes.push(...layout2D.notes);
  const real2DBoxes = Number(layout2D?.layout?.summary?.total_boxes || 0);
  const suggestedBoxesRaw = real2DBoxes > 0 ? real2DBoxes : suggestedBoxes;
  const suggestedBoxesFinal = Math.min(suggestedBoxesRaw, hardLimitBoxes);
  if (real2DBoxes > 0 && real2DBoxes !== suggestedBoxes) {
    notes.push(`Suggested boxes now follow Real 2D layout: ${real2DBoxes} (volume model was ${suggestedBoxes}).`);
  }
  if (suggestedBoxesRaw > hardLimitBoxes) {
    notes.push(`Suggested boxes capped by hard limit: ${hardLimitBoxes}.`);
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
      hard_limit_boxes: hardLimitBoxes,
      suggested_boxes_raw: suggestedBoxesRaw,
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

function trimItemsToHardLimit(items, boxM3, options = {}) {
  const hardLimitRaw = Number(options.maxBoxesHardLimit ?? process.env.AI_ARRANGE_HARDLIMIT_BOXES ?? 12);
  const hardLimitBoxes = Number.isFinite(hardLimitRaw) && hardLimitRaw > 0 ? Math.floor(hardLimitRaw) : 12;
  const maxUnitsForSolver = Number(options.maxUnitsForSolver || 2500);
  const maxIterations = Number(options.maxIterations || 200);

  let working = (items || [])
    .map((row) => ({ ...row, qty: toPositiveInt(row.qty) }))
    .filter((row) => Number(row.qty || 0) > 0);

  let packed = buildPackingFromResolvedItems(working, boxM3, { maxUnitsForSolver, maxBoxesHardLimit: hardLimitBoxes });
  let suggestedRaw = Number(packed?.totals?.suggested_boxes_raw || packed?.totals?.suggested_boxes || 0);
  let iter = 0;

  while (working.length > 0 && suggestedRaw > hardLimitBoxes && iter < maxIterations) {
    let targetIdx = -1;
    let targetScore = -1;
    for (let i = 0; i < working.length; i += 1) {
      const row = working[i];
      const qty = Number(row.qty || 0);
      if (qty <= 0) continue;
      const score = Number(row.unit_volume_m3 || 0) * qty;
      if (score > targetScore) {
        targetScore = score;
        targetIdx = i;
      }
    }
    if (targetIdx < 0) break;

    const onlyOneUnitLeft = working.length === 1 && Number(working[targetIdx].qty || 0) <= 1;
    if (onlyOneUnitLeft) break;
    working[targetIdx].qty = Number(working[targetIdx].qty || 0) - 1;
    if (working[targetIdx].qty <= 0) {
      working.splice(targetIdx, 1);
    }

    packed = buildPackingFromResolvedItems(working, boxM3, { maxUnitsForSolver, maxBoxesHardLimit: hardLimitBoxes });
    suggestedRaw = Number(packed?.totals?.suggested_boxes_raw || packed?.totals?.suggested_boxes || 0);
    iter += 1;
  }

  return {
    items: working,
    packed,
    hard_limit_boxes: hardLimitBoxes,
    iterations: iter,
  };
}

async function askOllamaForBranchPlan({
  branchId,
  maxProducts,
  lookbackOrders,
  baselineTotalQty,
  demandProducts,
  stockByProduct,
  specialInstruction,
}) {
  const stockBrief = stockByProduct.map((row) => ({
    product_id: Number(row.product_id),
    product_name: row.product_name,
    available_qty: Number(row.available_qty || 0),
    earliest_expiry: row.earliest_expiry,
  }));
  const historyBrief = demandProducts.map((row) => ({
    product_id: Number(row.product_id),
    product_name: row.product_name,
    avg_qty_from_history: Number(row.target_qty || 0),
  }));

  const systemPrompt = [
    "You are a logistics demand planner.",
    "Return ONLY valid JSON.",
    'JSON schema: {"products":[{"product_id":number,"target_qty":number}],"reason":"string"}',
    "Rules:",
    "- choose at most maxProducts products",
    "- target_qty must be integer >= 1",
    "- do not include products that are not in stockByProduct",
    "- prioritize products from demand history",
    "- total target_qty should be close to baselineTotalQty when possible",
  ].join("\n");

  const userPrompt = JSON.stringify(
    {
      branch_id: branchId,
      maxProducts,
      lookbackOrders,
      baselineTotalQty: Number(baselineTotalQty || 0),
      demandFromHistory: historyBrief,
      stockByProduct: stockBrief,
      specialInstruction: specialInstruction || "",
      outputRule: "Return JSON only. No markdown.",
    },
    null,
    2
  );

  const response = await fetch(`${OLLAMA_URL}/api/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: OLLAMA_MODEL,
      prompt: `${systemPrompt}\n\n${userPrompt}`,
      format: "json",
      stream: false,
      options: {
        temperature: 0.2,
      },
    }),
  });

  if (!response.ok) {
    throw new Error(`ollama http ${response.status}`);
  }
  const data = await response.json();
  const raw = String(data?.response || "").trim();
  if (!raw) throw new Error("ollama empty response");

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new Error("ollama non-json response");
  }

  if (!Array.isArray(parsed?.products)) {
    throw new Error("ollama response missing products array");
  }
  return parsed;
}

async function aiArrangeBranch({ branch_id, box_m3, max_products, lookback_orders, special_instruction }) {
  const branchId = Number(branch_id);
  const boxM3 = Number.isFinite(Number(box_m3)) && Number(box_m3) > 0 ? Number(box_m3) : DEFAULT_BOX_M3;
  const maxProducts = clampInt(max_products ?? 6, 1, 12);
  const lookbackOrders = clampInt(lookback_orders ?? 20, 1, 100);

  const [recentOrderTotals] = await pool.query(
    `SELECT
       o.id AS order_id,
       o.order_date,
       COALESCE(SUM(oi.qty), 0) AS total_qty
     FROM orders o
     LEFT JOIN order_items oi ON oi.order_id = o.id
     WHERE o.branch_id = ?
     GROUP BY o.id, o.order_date
     ORDER BY o.order_date DESC, o.id DESC
     LIMIT ?`,
    [branchId, lookbackOrders]
  );
  const latestOrderQty = Number(recentOrderTotals?.[0]?.total_qty || 0);
  const avgOrderQty = recentOrderTotals.length > 0
    ? Number(
        (
          recentOrderTotals.reduce((s, r) => s + Number(r.total_qty || 0), 0) /
          recentOrderTotals.length
        ).toFixed(2)
      )
    : 0;
  const baselineTotalQty = latestOrderQty > 0 ? latestOrderQty : (avgOrderQty > 0 ? avgOrderQty : 0);

  // Demand signal from recent orders of the branch (latest N orders).
  const [demandRows] = await pool.query(
    `SELECT
       oi.product_id,
       p.name AS product_name,
       p.volume_m3,
       SUM(oi.qty) AS total_qty,
       COUNT(DISTINCT ro.id) AS order_count,
       MAX(ro.order_date) AS last_order_date
     FROM (
       SELECT id, order_date
       FROM orders
       WHERE branch_id = ?
       ORDER BY order_date DESC, id DESC
       LIMIT ?
     ) ro
     JOIN order_items oi ON oi.order_id = ro.id
     JOIN products p ON p.product_id = oi.product_id
     GROUP BY oi.product_id, p.name, p.volume_m3
     ORDER BY total_qty DESC, last_order_date DESC
     LIMIT ?`,
    [branchId, lookbackOrders, maxProducts]
  );

  const demandProducts = demandRows.map((r) => ({
    product_id: Number(r.product_id),
    product_name: r.product_name,
    volume_m3: Number(r.volume_m3 || 0),
    target_qty: clampInt(Math.ceil(Number(r.total_qty || 0) / Math.max(Number(r.order_count || 1), 1)), 1, 40),
  }));

  const [stockAggRows] = await pool.query(
    `SELECT
       pl.product_id,
       p.name AS product_name,
       SUM(pl.remaining_qty) AS available_qty,
       MIN(pl.expiry_date) AS earliest_expiry
     FROM product_lots pl
     JOIN products p ON p.product_id = pl.product_id
     WHERE pl.remaining_qty > 0
     GROUP BY pl.product_id, p.name
     ORDER BY available_qty DESC`
  );

  let candidateProducts = demandProducts;
  let mode = "history";
  let aiProvider = "heuristic";
  let aiReason = "";

  // Fallback: no history yet -> use soonest-expiry available lots.
  if (candidateProducts.length === 0) {
    const [fallbackRows] = await pool.query(
      `SELECT
         p.product_id,
         p.name AS product_name,
         p.volume_m3,
         MAX(pl.remaining_qty) AS max_lot_qty
       FROM product_lots pl
       JOIN products p ON p.product_id = pl.product_id
       WHERE pl.remaining_qty > 0
       GROUP BY p.product_id, p.name, p.volume_m3
       ORDER BY p.product_id
       LIMIT ?`,
      [maxProducts]
    );

    candidateProducts = fallbackRows.map((r) => ({
      product_id: Number(r.product_id),
      product_name: r.product_name,
      volume_m3: Number(r.volume_m3 || 0),
      target_qty: clampInt(Math.min(Number(r.max_lot_qty || 0), 20), 1, 40),
    }));
    mode = "stock_fallback";
  }

  // Real LLM call: let Ollama pick demand mix and qty, then validate against stock.
  try {
    const ollamaPlan = await askOllamaForBranchPlan({
      branchId,
      maxProducts,
      lookbackOrders,
      baselineTotalQty,
      demandProducts,
      stockByProduct: stockAggRows,
      specialInstruction: special_instruction,
    });

    const stockMap = new Map(
      (stockAggRows || []).map((r) => [Number(r.product_id), toPositiveInt(r.available_qty)])
    );
    const demandMap = new Map(
      (demandProducts || []).map((r) => [Number(r.product_id), r.product_name])
    );
    const stockNameMap = new Map(
      (stockAggRows || []).map((r) => [Number(r.product_id), r.product_name])
    );

    const normalized = [];
    for (const row of ollamaPlan.products) {
      const pid = Number(row?.product_id);
      if (!Number.isInteger(pid) || pid <= 0) continue;
      if (!stockMap.has(pid)) continue;
      const available = toPositiveInt(stockMap.get(pid));
      if (available <= 0) continue;
      const qty = clampInt(toPositiveInt(row?.target_qty), 1, Math.max(1, available));
      normalized.push({
        product_id: pid,
        product_name: demandMap.get(pid) || stockNameMap.get(pid) || `Product #${pid}`,
        target_qty: qty,
      });
    }

    const dedupMap = new Map();
    for (const row of normalized) {
      if (!dedupMap.has(row.product_id)) {
        dedupMap.set(row.product_id, row);
      }
    }
    const picked = Array.from(dedupMap.values()).slice(0, maxProducts);

    if (picked.length > 0) {
      candidateProducts = picked;
      aiProvider = "ollama";
      aiReason = String(ollamaPlan?.reason || "").slice(0, 240);
      mode = demandProducts.length > 0 ? "history+ollama" : "stock_fallback+ollama";
    } else {
      aiReason = "ollama returned no valid products; fallback heuristic used";
    }
  } catch (err) {
    aiReason = `ollama unavailable (${String(err.message || err)})`;
  }

  if (candidateProducts.length === 0) {
    throw new Error("no product candidates for AI arrangement");
  }

  const availableByProduct = new Map(
    (stockAggRows || []).map((r) => [Number(r.product_id), toPositiveInt(r.available_qty)])
  );
  candidateProducts = rebalanceCandidateQuantities(candidateProducts, baselineTotalQty, availableByProduct);

  const productIds = candidateProducts.map((p) => Number(p.product_id));
  const [lotRows] = await pool.query(
    `SELECT
       pl.lot_id,
       pl.product_id,
       pl.lot_number,
       pl.expiry_date,
       pl.remaining_qty,
       p.name AS product_name,
       p.volume_m3,
       p.width_cm,
       p.length_cm,
       p.height_cm
     FROM product_lots pl
     JOIN products p ON p.product_id = pl.product_id
     WHERE pl.remaining_qty > 0
       AND pl.product_id IN (${productIds.map(() => "?").join(",")})
     ORDER BY pl.expiry_date ASC, pl.lot_id ASC`,
    productIds
  );

  const lotsByProduct = new Map();
  for (const lot of lotRows) {
    const pid = Number(lot.product_id);
    if (!lotsByProduct.has(pid)) lotsByProduct.set(pid, []);
    lotsByProduct.get(pid).push({
      lot_id: Number(lot.lot_id),
      product_id: pid,
      product_name: lot.product_name,
      lot_number: lot.lot_number,
      expiry_date: lot.expiry_date,
      remaining_qty: Number(lot.remaining_qty || 0),
      volume_m3: Number(lot.volume_m3 || 0),
      width_cm: Number(lot.width_cm || 0),
      length_cm: Number(lot.length_cm || 0),
      height_cm: Number(lot.height_cm || 0),
    });
  }

  let items = [];
  for (const product of candidateProducts) {
    let needQty = Number(product.target_qty || 0);
    if (needQty <= 0) continue;
    const lots = lotsByProduct.get(Number(product.product_id)) || [];
    for (const lot of lots) {
      if (needQty <= 0) break;
      const take = Math.min(needQty, Number(lot.remaining_qty || 0));
      if (take <= 0) continue;
      items.push({
        lot_id: Number(lot.lot_id),
        product_id: Number(product.product_id),
        product_name: product.product_name,
        lot_number: lot.lot_number,
        expiry_date: lot.expiry_date,
        qty: Number(take),
        unit_volume_m3: Number(lot.volume_m3 || 0),
        unit_width_cm: Number(lot.width_cm || 0),
        unit_length_cm: Number(lot.length_cm || 0),
        unit_height_cm: Number(lot.height_cm || 0),
      });
      needQty -= take;
    }
  }

  if (items.length === 0) {
    throw new Error("no available lots to fulfill AI arrangement");
  }

  const trimmed = trimItemsToHardLimit(items, boxM3, {
    maxUnitsForSolver: 2500,
    maxBoxesHardLimit: Number(process.env.AI_ARRANGE_HARDLIMIT_BOXES || 12),
    maxIterations: 400,
  });
  items = trimmed.items;
  const packed = trimmed.packed;
  const finalTotalQty = items.reduce((s, x) => s + Number(x.qty || 0), 0);
  const qtyGap = Number((finalTotalQty - baselineTotalQty).toFixed(2));
  const qtyMatchPct = baselineTotalQty > 0
    ? Number((Math.min(finalTotalQty, baselineTotalQty) / baselineTotalQty * 100).toFixed(1))
    : 100;
  const notes = [
    "Items are auto-picked from branch demand history and current stock (FEFO by expiry).",
    "Suggested boxes use Real 2D layout when available; otherwise fallback to volume (Bee-inspired) model.",
  ];
  if (baselineTotalQty > 0) {
    notes.push(`Quantity target aligned to previous orders: baseline=${baselineTotalQty}, planned=${finalTotalQty}, match=${qtyMatchPct}%.`);
  }
  if (trimmed.iterations > 0) {
    notes.push(`AI quantities auto-adjusted to fit hard limit ${trimmed.hard_limit_boxes} boxes.`);
  }
  notes.push(...packed.notes);

  return {
    branch_id: branchId,
    mode,
    ai_provider: aiProvider,
    ai_reason: aiReason,
    ollama: {
      url: OLLAMA_URL,
      model: OLLAMA_MODEL,
      used: aiProvider === "ollama",
    },
    items: items.map((x) => ({
      lot_id: x.lot_id,
      qty: x.qty,
      product_id: x.product_id,
      product_name: x.product_name,
      lot_number: x.lot_number,
      expiry_date: x.expiry_date,
    })),
    totals: {
      ...packed.totals,
    },
    quantity_alignment: {
      baseline_qty: Number(baselineTotalQty || 0),
      latest_order_qty: Number(latestOrderQty || 0),
      avg_order_qty: Number(avgOrderQty || 0),
      planned_qty: Number(finalTotalQty || 0),
      qty_gap: qtyGap,
      match_pct: qtyMatchPct,
    },
    packing: packed.packing,
    notes,
  };
}

async function debugPackBranchItems({ branch_id, box_m3, items }) {
  const boxM3 = Number.isFinite(Number(box_m3)) && Number(box_m3) > 0 ? Number(box_m3) : DEFAULT_BOX_M3;
  const lotIds = Array.from(new Set((items || []).map((x) => Number(x.lot_id)).filter((id) => Number.isInteger(id) && id > 0)));
  if (lotIds.length === 0) {
    throw new Error("items must contain at least one valid lot_id");
  }

  const [lotRows] = await pool.query(
    `SELECT
       pl.lot_id,
       pl.product_id,
       pl.lot_number,
       pl.expiry_date,
       pl.remaining_qty,
       p.name AS product_name,
       p.volume_m3,
       p.width_cm,
       p.length_cm,
       p.height_cm
     FROM product_lots pl
     JOIN products p ON p.product_id = pl.product_id
     WHERE pl.lot_id IN (${lotIds.map(() => "?").join(",")})`,
    lotIds
  );
  const lotMap = new Map(lotRows.map((r) => [Number(r.lot_id), r]));

  const resolvedItems = [];
  for (const row of items || []) {
    const lotId = Number(row?.lot_id);
    const qty = Number(row?.qty);
    if (!Number.isInteger(lotId) || lotId <= 0) continue;
    if (!Number.isInteger(qty) || qty <= 0) continue;
    const lot = lotMap.get(lotId);
    if (!lot) throw new Error(`lot_id not found: ${lotId}`);
    resolvedItems.push({
      lot_id: lotId,
      qty,
      product_id: Number(lot.product_id),
      product_name: lot.product_name,
      lot_number: lot.lot_number,
      expiry_date: lot.expiry_date,
      unit_volume_m3: Number(lot.volume_m3 || 0),
      unit_width_cm: Number(lot.width_cm || 0),
      unit_length_cm: Number(lot.length_cm || 0),
      unit_height_cm: Number(lot.height_cm || 0),
    });
  }

  if (resolvedItems.length === 0) {
    throw new Error("items must contain at least one valid qty");
  }

  const packed = buildPackingFromResolvedItems(resolvedItems, boxM3, { maxUnitsForSolver: 2500 });
  return {
    branch_id: Number(branch_id || 0),
    mode: "manual_debug",
    items: resolvedItems.map((x) => ({
      lot_id: x.lot_id,
      qty: x.qty,
      product_id: x.product_id,
      product_name: x.product_name,
      lot_number: x.lot_number,
      expiry_date: x.expiry_date,
    })),
    totals: packed.totals,
    packing: packed.packing,
    notes: packed.notes,
  };
}

async function debugCompareExistingOrder({ tx_id, order_id, box_m3 }) {
  const txId = Number(tx_id);
  const orderId = Number(order_id);
  const boxM3 = Number.isFinite(Number(box_m3)) && Number(box_m3) > 0 ? Number(box_m3) : DEFAULT_BOX_M3;

  const selected = await getTxSelected(txId);
  const order = (selected?.orders || []).find((o) => Number(o.order_id) === orderId);
  if (!order) {
    throw new Error(`order_id ${orderId} not found in tx_id ${txId}`);
  }

  const manualItems = (order.items || [])
    .map((it) => ({
      lot_id: Number(it.lot_id || 0),
      qty: Number(it.qty || 0),
      product_id: Number(it.product_id || 0),
      product_name: it.product_name || `Product #${it.product_id}`,
      lot_number: it.lot_number || null,
      expiry_date: it.expiry_date || null,
      unit_volume_m3: Number(it.volume_m3 || 0),
      unit_width_cm: 0,
      unit_length_cm: 0,
      unit_height_cm: 0,
    }))
    .filter((it) => Number.isInteger(it.qty) && it.qty > 0 && Number.isFinite(it.unit_volume_m3) && it.unit_volume_m3 >= 0);

  if (manualItems.length === 0) {
    throw new Error(`order_id ${orderId} has no comparable items`);
  }

  const manualProductIds = Array.from(new Set(manualItems.map((it) => Number(it.product_id)).filter((id) => Number.isInteger(id) && id > 0)));
  if (manualProductIds.length > 0) {
    const [dimRows] = await pool.query(
      `SELECT product_id, width_cm, length_cm, height_cm
       FROM products
       WHERE product_id IN (${manualProductIds.map(() => "?").join(",")})`,
      manualProductIds
    );
    const dimMap = new Map(dimRows.map((r) => [
      Number(r.product_id),
      {
        width_cm: Number(r.width_cm || 0),
        length_cm: Number(r.length_cm || 0),
        height_cm: Number(r.height_cm || 0),
      },
    ]));
    for (const item of manualItems) {
      const dim = dimMap.get(Number(item.product_id));
      if (!dim) continue;
      item.unit_width_cm = Number(dim.width_cm || 0);
      item.unit_length_cm = Number(dim.length_cm || 0);
      item.unit_height_cm = Number(dim.height_cm || 0);
    }
  }

  const manualPacked = buildPackingFromResolvedItems(manualItems, boxM3, { maxUnitsForSolver: 2500 });
  const aiPacked = await aiArrangeBranch({
    branch_id: Number(order.branch_id),
    box_m3: boxM3,
    max_products: 12,
    lookback_orders: 30,
    special_instruction: `Debug compare with existing order_id ${orderId}`,
  });

  const manualBoxes = Number(manualPacked.packing?.summary?.total_boxes || 0);
  const aiBoxes = Number(aiPacked?.packing?.summary?.total_boxes || 0);
  const manualUtil = Number(manualPacked.packing?.summary?.avg_utilization_pct || 0);
  const aiUtil = Number(aiPacked?.packing?.summary?.avg_utilization_pct || 0);
  const manualQty = Number(manualPacked.totals?.total_qty || 0);
  const aiQty = Number(aiPacked?.totals?.total_qty || 0);

  return {
    tx_id: txId,
    order_id: orderId,
    branch_id: Number(order.branch_id),
    compare_basis: {
      box_m3: boxM3,
      box_dimension_hint: "40x60x15 cm",
    },
    manual: {
      totals: manualPacked.totals,
      packing: manualPacked.packing,
      notes: manualPacked.notes,
    },
    ai: {
      mode: aiPacked.mode,
      ai_provider: aiPacked.ai_provider,
      ai_reason: aiPacked.ai_reason,
      quantity_alignment: aiPacked.quantity_alignment || null,
      totals: aiPacked.totals,
      packing: aiPacked.packing,
      notes: aiPacked.notes,
    },
    comparison: {
      boxes_saved_by_ai: manualBoxes - aiBoxes,
      avg_utilization_diff_pct: Number((aiUtil - manualUtil).toFixed(1)),
      qty_diff: aiQty - manualQty,
      winner_by_boxes: aiBoxes < manualBoxes ? "AI" : aiBoxes > manualBoxes ? "MANUAL" : "TIE",
    },
  };
}

async function listTx(limit = 50) {
  const safe = toSafeLimit(limit, 50);
  const [rows] = await pool.query(
    `SELECT
       t.id AS tx_id,
       t.tx_date,
       t.status,
       t.box_m3,
       t.note,
       t.created_at,
       rl.id AS route_id,
       lf.name AS from_name,
       lt.name AS to_name
     FROM tx t
     LEFT JOIN tx_legs ll
       ON ll.tx_id = t.id
      AND ll.leg_type = 'LINEHAUL'
     LEFT JOIN routes rl
       ON rl.id = ll.route_id
     LEFT JOIN locations lf
       ON lf.id = rl.from_location_id
     LEFT JOIN locations lt
       ON lt.id = rl.to_location_id
     ORDER BY t.id DESC
     LIMIT ?`,
    [safe]
  );
  return rows;
}

async function txExists(txId) {
  const [r] = await pool.query("SELECT 1 FROM tx WHERE id = ? LIMIT 1", [txId]);
  return r.length > 0;
}

async function vehicleExists(vehicleId) {
  const [r] = await pool.query("SELECT 1 FROM vehicles WHERE id = ? LIMIT 1", [vehicleId]);
  return r.length > 0;
}

async function routeExists(routeId) {
  const [r] = await pool.query("SELECT 1 FROM routes WHERE id = ? LIMIT 1", [routeId]);
  return r.length > 0;
}

async function distributorExists(dcId) {
  const [r] = await pool.query("SELECT 1 FROM distributors WHERE id = ? LIMIT 1", [dcId]);
  return r.length > 0;
}

async function branchExists(branchId) {
  const [r] = await pool.query("SELECT 1 FROM branches WHERE id = ? LIMIT 1", [branchId]);
  return r.length > 0;
}

async function productExists(productId) {
  const [r] = await pool.query("SELECT 1 FROM products WHERE product_id = ? LIMIT 1", [productId]);
  return r.length > 0;
}

async function getProductLotById(lotId) {
  const [rows] = await pool.query(
    `SELECT lot_id, product_id, lot_number, expiry_date, remaining_qty
     FROM product_lots
     WHERE lot_id = ?
     LIMIT 1`,
    [lotId]
  );
  return rows[0] || null;
}

async function getVehicleById(vehicleId) {
  const [rows] = await pool.query(
    "SELECT id, name, vehicle_type, capacity_boxes, status FROM vehicles WHERE id = ? LIMIT 1",
    [vehicleId]
  );
  return rows[0] || null;
}

async function getEmployeeById(employeeId) {
  const [rows] = await pool.query(
    "SELECT employee_id, firstname, lastname, role FROM employees WHERE employee_id = ? LIMIT 1",
    [employeeId]
  );
  return rows[0] || null;
}

async function createTx({ tx_date, note, box_m3 }) {
  const b = box_m3 === undefined ? 0.036 : Number(box_m3);
  const [result] = await pool.query(
    "INSERT INTO tx (tx_date, status, box_m3, note) VALUES (?, 'DRAFT', ?, ?)",
    [tx_date, b, note]
  );
  return await getTxById(result.insertId);
}

async function getTxById(txId) {
  const [rows] = await pool.query(
    `SELECT
       t.id,
       t.id AS tx_id,
       t.tx_date,
       t.status,
       t.box_m3,
       t.note,
       t.created_at,
       rl.id AS route_id,
       lf.name AS from_name,
       lt.name AS to_name
     FROM tx t
     LEFT JOIN tx_legs ll
       ON ll.tx_id = t.id
      AND ll.leg_type = 'LINEHAUL'
     LEFT JOIN routes rl
       ON rl.id = ll.route_id
     LEFT JOIN locations lf
       ON lf.id = rl.from_location_id
     LEFT JOIN locations lt
       ON lt.id = rl.to_location_id
     WHERE t.id = ?`,
    [txId]
  );
  return rows[0] || null;
}

async function updateTxDraft(txId, { tx_date, box_m3, note }) {
  await pool.query(
    `UPDATE tx
     SET
       tx_date = COALESCE(?, tx_date),
       box_m3 = COALESCE(?, box_m3),
       note = COALESCE(?, note)
     WHERE id = ?
       AND status = 'DRAFT'`,
    [tx_date ?? null, box_m3 ?? null, note ?? null, txId]
  );
  return await getTxById(txId);
}

async function updateTxStatus(txId, status) {
  await pool.query(
    `UPDATE tx
     SET status = ?
     WHERE id = ?`,
    [status, txId]
  );
  return await getTxById(txId);
}

async function addTxLeg({ tx_id, leg_type, vehicle_id, route_id, dc_id, driver_id, sale_id }) {
  const driver = await getEmployeeById(driver_id);
  if (!driver) throw new Error("driver_id not found");
  if (String(driver.role || "").toUpperCase() !== "DRIVER") {
    throw new Error("driver_id must be Driver");
  }

  const sale = await getEmployeeById(sale_id);
  if (!sale) throw new Error("sale_id not found");
  if (String(sale.role || "").toUpperCase() !== "SALE") {
    throw new Error("sale_id must be Sale");
  }

  const [result] = await pool.query(
    `INSERT INTO tx_legs (tx_id, leg_type, vehicle_id, route_id, dc_id, driver_id, sale_id)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [tx_id, leg_type, vehicle_id, route_id, dc_id, driver_id ?? null, sale_id ?? null]
  );

  const [rows] = await pool.query(
    `SELECT id, tx_id, leg_type, vehicle_id, route_id, dc_id, driver_id, sale_id
     FROM tx_legs WHERE id = ?`,
    [result.insertId]
  );
  return rows[0];
}

// orders: ต้องมีอยู่และเป็น OPEN เท่านั้น
async function findInvalidOrders(orderIds) {
  if (orderIds.length === 0) return [];
  const [rows] = await pool.query(
    `SELECT id FROM orders WHERE id IN (${orderIds.map(() => "?").join(",")}) AND status = 'OPEN'`,
    orderIds
  );
  const okSet = new Set(rows.map((x) => Number(x.id)));
  return orderIds.filter((id) => !okSet.has(Number(id)));
}

async function addOrdersToTx(txId, orderIds) {
  let added = 0;
  const addedOrderIds = [];

  for (const orderId of orderIds) {
    const [r] = await pool.query(
      "INSERT IGNORE INTO tx_orders (tx_id, order_id) VALUES (?, ?)",
      [txId, orderId]
    );
    const a = Number(r.affectedRows || 0);
    if (a === 1) {
      added += 1;
      addedOrderIds.push(orderId);
    }
  }

  return { added, added_order_ids: addedOrderIds };
}

async function getOpenOrderIdsByBranch(branchId) {
  const [rows] = await pool.query(
    "SELECT id FROM orders WHERE branch_id = ? AND status = 'OPEN' ORDER BY id",
    [branchId]
  );
  return rows.map((r) => Number(r.id));
}

async function addTxReturns(txId, items) {
  // items: [{branch_id, product_id, qty, lot_code?, expiry_date?}]
  let inserted = 0;
  for (const it of items) {
    const [r] = await pool.query(
      `INSERT INTO tx_returns (tx_id, branch_id, product_id, qty, lot_code, expiry_date)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        txId,
        Number(it.branch_id),
        String(it.product_id),
        Number(it.qty),
        it.lot_code ?? null,
        it.expiry_date ?? null,
      ]
    );
    inserted += Number(r.affectedRows || 0);
  }
  return inserted;
}

async function previewOrderPlan({ branches, box_m3 }) {
  const boxM3 = box_m3 === undefined ? 0.036 : Number(box_m3);
  const lotIds = new Set();

  for (const branch of branches) {
    for (const item of branch.items) {
      lotIds.add(Number(item.lot_id));
    }
  }

  const ids = Array.from(lotIds);
  const [lotRows] = ids.length
    ? await pool.query(
      `SELECT
         pl.lot_id,
         pl.product_id,
         p.volume_m3
       FROM product_lots pl
       JOIN products p ON p.product_id = pl.product_id
       WHERE pl.lot_id IN (${ids.map(() => "?").join(",")})`,
      ids
    )
    : [[]];

  const volumeByLotId = new Map(lotRows.map((r) => [Number(r.lot_id), Number(r.volume_m3 || 0)]));
  for (const lotId of ids) {
    if (!volumeByLotId.has(Number(lotId))) throw new Error(`lot_id not found: ${lotId}`);
  }
  let totalM3 = 0;
  const byBranch = [];

  for (const branch of branches) {
    let branchM3 = 0;
    for (const item of branch.items) {
      const lotId = Number(item.lot_id);
      const qty = Number(item.qty);
      const unit = Number(volumeByLotId.get(lotId) || 0);
      branchM3 += qty * unit;
    }
    totalM3 += branchM3;
    byBranch.push({
      branch_id: Number(branch.branch_id),
      total_m3: branchM3,
      total_boxes: Math.ceil(branchM3 / boxM3),
    });
  }

  return {
    totals: {
      box_m3: boxM3,
      total_m3: totalM3,
      total_boxes: Math.ceil(totalM3 / boxM3),
    },
    by_branch: byBranch,
  };
}

async function createFromOrderPlan({
  tx_date,
  route_id,
  dc_id,
  linehaul_vehicle_id,
  linehaul_driver_id,
  linehaul_sale_id,
  lastmile_vehicle_id,
  lastmile_driver_id,
  lastmile_sale_id,
  box_m3,
  note,
  branches,
}) {
  const boxM3 = box_m3 === undefined ? 0.036 : Number(box_m3);
  const vehicle = await getVehicleById(linehaul_vehicle_id);
  if (!vehicle) throw new Error("linehaul_vehicle_id not found");
  if (String(vehicle.vehicle_type || "").toUpperCase() !== "LINEHAUL") {
    throw new Error("linehaul_vehicle_id must be LINEHAUL");
  }
  if (String(vehicle.status || "").toUpperCase() !== "AVAILABLE") {
    throw new Error("linehaul vehicle is not AVAILABLE");
  }

  const lastmileVehicle = await getVehicleById(lastmile_vehicle_id);
  if (!lastmileVehicle) throw new Error("lastmile_vehicle_id not found");
  if (String(lastmileVehicle.vehicle_type || "").toUpperCase() !== "LASTMILE") {
    throw new Error("lastmile_vehicle_id must be LASTMILE");
  }
  if (String(lastmileVehicle.status || "").toUpperCase() !== "AVAILABLE") {
    throw new Error("lastmile vehicle is not AVAILABLE");
  }

  const linehaulDriver = await getEmployeeById(linehaul_driver_id);
  if (!linehaulDriver) throw new Error("linehaul_driver_id not found");
  if (String(linehaulDriver.role || "").toUpperCase() !== "DRIVER") {
    throw new Error("linehaul_driver_id must be Driver");
  }

  const linehaulSale = await getEmployeeById(linehaul_sale_id);
  if (!linehaulSale) throw new Error("linehaul_sale_id not found");
  if (String(linehaulSale.role || "").toUpperCase() !== "SALE") {
    throw new Error("linehaul_sale_id must be Sale");
  }

  const lastmileDriver = await getEmployeeById(lastmile_driver_id);
  if (!lastmileDriver) throw new Error("lastmile_driver_id not found");
  if (String(lastmileDriver.role || "").toUpperCase() !== "DRIVER") {
    throw new Error("lastmile_driver_id must be Driver");
  }

  const lastmileSale = await getEmployeeById(lastmile_sale_id);
  if (!lastmileSale) throw new Error("lastmile_sale_id not found");
  if (String(lastmileSale.role || "").toUpperCase() !== "SALE") {
    throw new Error("lastmile_sale_id must be Sale");
  }

  if (Number(linehaul_driver_id) === Number(lastmile_driver_id)) {
    throw new Error("linehaul_driver_id cannot be reused as lastmile_driver_id");
  }
  if (Number(linehaul_sale_id) === Number(lastmile_sale_id)) {
    throw new Error("linehaul_sale_id cannot be reused as lastmile_sale_id");
  }

  const branchIds = branches.map((b) => Number(b.branch_id));
  const [branchRows] = branchIds.length
    ? await pool.query(
      `SELECT id, distributor_id FROM branches WHERE id IN (${branchIds.map(() => "?").join(",")})`,
      branchIds
    )
    : [[]];

  const branchMap = new Map(branchRows.map((b) => [Number(b.id), Number(b.distributor_id)]));
  for (const bid of branchIds) {
    if (!branchMap.has(Number(bid))) throw new Error(`branch_id not found: ${bid}`);
    if (branchMap.get(Number(bid)) !== Number(dc_id)) {
      throw new Error(`branch_id ${bid} does not belong to dc_id ${dc_id}`);
    }
  }

  const lotIds = new Set();
  for (const branch of branches) {
    for (const item of branch.items) {
      lotIds.add(Number(item.lot_id));
    }
  }

  const [lotRows] = lotIds.size
    ? await pool.query(
      `SELECT
         pl.lot_id,
         pl.product_id,
         pl.remaining_qty,
         p.volume_m3
       FROM product_lots pl
       JOIN products p ON p.product_id = pl.product_id
       WHERE pl.lot_id IN (${Array.from(lotIds).map(() => "?").join(",")})`,
      Array.from(lotIds)
    )
    : [[]];

  const lotMap = new Map(lotRows.map((r) => [Number(r.lot_id), r]));
  for (const lotId of lotIds) {
    if (!lotMap.has(Number(lotId))) throw new Error(`lot_id not found: ${lotId}`);
  }

  const productIds = new Set(lotRows.map((r) => Number(r.product_id)));
  const ids = Array.from(productIds);
  const [productRows] = ids.length
    ? await pool.query(
      `SELECT product_id, volume_m3 FROM products WHERE product_id IN (${ids.map(() => "?").join(",")})`,
      ids
    )
    : [[]];
  const productMap = new Map(productRows.map((p) => [Number(p.product_id), Number(p.volume_m3 || 0)]));
  for (const pid of productIds) {
    if (!productMap.has(Number(pid))) throw new Error(`product_id not found: ${pid}`);
  }

  const requestedByLot = new Map();
  for (const branch of branches) {
    for (const item of branch.items) {
      const lotId = Number(item.lot_id);
      const qty = Number(item.qty);
      requestedByLot.set(lotId, (requestedByLot.get(lotId) || 0) + qty);
    }
  }
  for (const [lotId, needQty] of requestedByLot.entries()) {
    const lot = lotMap.get(Number(lotId));
    if (!lot) throw new Error(`lot_id not found: ${lotId}`);
    if (Number(lot.remaining_qty || 0) < Number(needQty)) {
      throw new Error(`insufficient lot stock: lot_id ${lotId}`);
    }
  }

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    const [txResult] = await conn.query(
      "INSERT INTO tx (tx_date, status, box_m3, note) VALUES (?, 'DRAFT', ?, ?)",
      [tx_date, boxM3, note ?? null]
    );
    const txId = Number(txResult.insertId);

    await conn.query(
      `INSERT INTO tx_legs (tx_id, leg_type, vehicle_id, route_id, dc_id, driver_id, sale_id)
       VALUES (?, 'LINEHAUL', ?, ?, ?, ?, ?)`,
      [
        txId,
        Number(linehaul_vehicle_id),
        Number(route_id),
        Number(dc_id),
        Number(linehaul_driver_id),
        Number(linehaul_sale_id),
      ]
    );

    await conn.query(
      `INSERT INTO tx_legs (tx_id, leg_type, vehicle_id, route_id, dc_id, driver_id, sale_id)
       VALUES (?, 'LASTMILE', ?, NULL, ?, ?, ?)`,
      [
        txId,
        Number(lastmile_vehicle_id),
        Number(dc_id),
        Number(lastmile_driver_id),
        Number(lastmile_sale_id),
      ]
    );

    const createdOrderIds = [];
    let totalM3 = 0;
    let totalQty = 0;

    for (const branch of branches) {
      const [orderResult] = await conn.query(
        "INSERT INTO orders (branch_id, order_date, status) VALUES (?, ?, 'OPEN')",
        [Number(branch.branch_id), tx_date]
      );
      const orderId = Number(orderResult.insertId);
      createdOrderIds.push(orderId);

      for (const item of branch.items) {
        const lotId = Number(item.lot_id);
        const lot = lotMap.get(lotId);
        if (!lot) throw new Error(`lot_id not found: ${lotId}`);
        const pid = Number(lot.product_id);
        const qty = Number(item.qty);
        await conn.query(
          "INSERT INTO order_items (order_id, product_id, lot_id, qty) VALUES (?, ?, ?, ?)",
          [orderId, pid, lotId, qty]
        );
        totalM3 += qty * Number(productMap.get(pid) || 0);
        totalQty += qty;
      }

      await conn.query(
        "INSERT INTO tx_orders (tx_id, order_id, driver_id, sale_id) VALUES (?, ?, ?, ?)",
        [txId, orderId, Number(lastmile_driver_id), Number(lastmile_sale_id)]
      );
    }

    for (const [lotId, needQty] of requestedByLot.entries()) {
      const [u] = await conn.query(
        `UPDATE product_lots
         SET remaining_qty = remaining_qty - ?
         WHERE lot_id = ?
           AND remaining_qty >= ?`,
        [Number(needQty), Number(lotId), Number(needQty)]
      );
      if (Number(u.affectedRows || 0) !== 1) {
        throw new Error(`insufficient lot stock while reserving lot_id ${lotId}`);
      }
    }

    await conn.query(
      "UPDATE vehicles SET status = 'IN_USE' WHERE id = ?",
      [Number(linehaul_vehicle_id)]
    );
    await conn.query(
      "UPDATE vehicles SET status = 'IN_USE' WHERE id = ?",
      [Number(lastmile_vehicle_id)]
    );

    await conn.commit();

    return {
      tx_id: txId,
      order_ids: createdOrderIds,
      lastmile_vehicle_id: Number(lastmile_vehicle_id),
      linehaul_driver_id: Number(linehaul_driver_id),
      linehaul_sale_id: Number(linehaul_sale_id),
      lastmile_driver_id: Number(lastmile_driver_id),
      lastmile_sale_id: Number(lastmile_sale_id),
      totals: {
        box_m3: boxM3,
        total_qty: totalQty,
        total_m3: totalM3,
        total_boxes: Math.ceil(totalM3 / boxM3),
      },
    };
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

async function replaceDraftFromOrderPlan({
  tx_id,
  tx_date,
  route_id,
  dc_id,
  linehaul_vehicle_id,
  linehaul_driver_id,
  linehaul_sale_id,
  lastmile_vehicle_id,
  lastmile_driver_id,
  lastmile_sale_id,
  box_m3,
  note,
  branches,
}) {
  const txId = Number(tx_id);
  const existingTx = await getTxById(txId);
  if (!existingTx) throw new Error("tx not found");
  if (String(existingTx.status || "").toUpperCase() !== "DRAFT") {
    throw new Error("only DRAFT tx can be edited");
  }

  const boxM3 = box_m3 === undefined ? 0.036 : Number(box_m3);
  const vehicle = await getVehicleById(linehaul_vehicle_id);
  if (!vehicle) throw new Error("linehaul_vehicle_id not found");
  if (String(vehicle.vehicle_type || "").toUpperCase() !== "LINEHAUL") {
    throw new Error("linehaul_vehicle_id must be LINEHAUL");
  }
  if (!["AVAILABLE", "IN_USE"].includes(String(vehicle.status || "").toUpperCase())) {
    throw new Error("linehaul vehicle is not available for editing");
  }

  const lastmileVehicle = await getVehicleById(lastmile_vehicle_id);
  if (!lastmileVehicle) throw new Error("lastmile_vehicle_id not found");
  if (String(lastmileVehicle.vehicle_type || "").toUpperCase() !== "LASTMILE") {
    throw new Error("lastmile_vehicle_id must be LASTMILE");
  }
  if (!["AVAILABLE", "IN_USE"].includes(String(lastmileVehicle.status || "").toUpperCase())) {
    throw new Error("lastmile vehicle is not available for editing");
  }

  const linehaulDriver = await getEmployeeById(linehaul_driver_id);
  if (!linehaulDriver) throw new Error("linehaul_driver_id not found");
  if (String(linehaulDriver.role || "").toUpperCase() !== "DRIVER") {
    throw new Error("linehaul_driver_id must be Driver");
  }

  const linehaulSale = await getEmployeeById(linehaul_sale_id);
  if (!linehaulSale) throw new Error("linehaul_sale_id not found");
  if (String(linehaulSale.role || "").toUpperCase() !== "SALE") {
    throw new Error("linehaul_sale_id must be Sale");
  }

  const lastmileDriver = await getEmployeeById(lastmile_driver_id);
  if (!lastmileDriver) throw new Error("lastmile_driver_id not found");
  if (String(lastmileDriver.role || "").toUpperCase() !== "DRIVER") {
    throw new Error("lastmile_driver_id must be Driver");
  }

  const lastmileSale = await getEmployeeById(lastmile_sale_id);
  if (!lastmileSale) throw new Error("lastmile_sale_id not found");
  if (String(lastmileSale.role || "").toUpperCase() !== "SALE") {
    throw new Error("lastmile_sale_id must be Sale");
  }

  if (Number(linehaul_driver_id) === Number(lastmile_driver_id)) {
    throw new Error("linehaul_driver_id cannot be reused as lastmile_driver_id");
  }
  if (Number(linehaul_sale_id) === Number(lastmile_sale_id)) {
    throw new Error("linehaul_sale_id cannot be reused as lastmile_sale_id");
  }

  const branchIds = branches.map((b) => Number(b.branch_id));
  const [branchRows] = branchIds.length
    ? await pool.query(
      `SELECT id, distributor_id FROM branches WHERE id IN (${branchIds.map(() => "?").join(",")})`,
      branchIds
    )
    : [[]];

  const branchMap = new Map(branchRows.map((b) => [Number(b.id), Number(b.distributor_id)]));
  for (const bid of branchIds) {
    if (!branchMap.has(Number(bid))) throw new Error(`branch_id not found: ${bid}`);
    if (branchMap.get(Number(bid)) !== Number(dc_id)) {
      throw new Error(`branch_id ${bid} does not belong to dc_id ${dc_id}`);
    }
  }

  const lotIds = new Set();
  for (const branch of branches) {
    for (const item of branch.items) {
      lotIds.add(Number(item.lot_id));
    }
  }

  const requestedByLot = new Map();
  for (const branch of branches) {
    for (const item of branch.items) {
      const lotId = Number(item.lot_id);
      const qty = Number(item.qty);
      requestedByLot.set(lotId, (requestedByLot.get(lotId) || 0) + qty);
    }
  }

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    const [oldLegRows] = await conn.query(
      "SELECT vehicle_id FROM tx_legs WHERE tx_id = ?",
      [txId]
    );
    const oldVehicleIds = Array.from(
      new Set((oldLegRows || []).map((r) => Number(r.vehicle_id)).filter((x) => Number.isInteger(x) && x > 0))
    );

    const [oldOrderRows] = await conn.query(
      "SELECT order_id FROM tx_orders WHERE tx_id = ?",
      [txId]
    );
    const oldOrderIds = (oldOrderRows || []).map((r) => Number(r.order_id)).filter((x) => Number.isInteger(x) && x > 0);

    const [oldReservedItemRows] = await conn.query(
      `SELECT oi.lot_id, oi.qty
       FROM tx_orders xo
       JOIN order_items oi ON oi.order_id = xo.order_id
       WHERE xo.tx_id = ?
         AND oi.lot_id IS NOT NULL
       FOR UPDATE`,
      [txId]
    );
    const oldReservedByLot = new Map();
    for (const row of oldReservedItemRows || []) {
      const lotId = Number(row.lot_id);
      const qty = Number(row.qty || 0);
      if (!Number.isInteger(lotId) || lotId <= 0) continue;
      if (!(qty > 0)) continue;
      oldReservedByLot.set(lotId, (oldReservedByLot.get(lotId) || 0) + qty);
    }

    const lockLotIds = Array.from(new Set([
      ...Array.from(lotIds).map((x) => Number(x)),
      ...Array.from(oldReservedByLot.keys()).map((x) => Number(x)),
    ])).filter((x) => Number.isInteger(x) && x > 0);

    const [lotRows] = lockLotIds.length
      ? await conn.query(
        `SELECT
           pl.lot_id,
           pl.product_id,
           pl.remaining_qty,
           p.volume_m3
         FROM product_lots pl
         JOIN products p ON p.product_id = pl.product_id
         WHERE pl.lot_id IN (${lockLotIds.map(() => "?").join(",")})
         FOR UPDATE`,
        lockLotIds
      )
      : [[]];
    const lotMap = new Map(lotRows.map((r) => [Number(r.lot_id), r]));

    for (const lotId of lotIds) {
      if (!lotMap.has(Number(lotId))) throw new Error(`lot_id not found: ${lotId}`);
    }
    for (const lotId of oldReservedByLot.keys()) {
      if (!lotMap.has(Number(lotId))) throw new Error(`lot_id not found: ${lotId}`);
    }

    const productMap = new Map();
    for (const row of lotRows || []) {
      productMap.set(Number(row.product_id), Number(row.volume_m3 || 0));
    }

    validateEditableLotAvailability(requestedByLot, oldReservedByLot, lotMap);

    await conn.query("DELETE FROM tx_legs WHERE tx_id = ?", [txId]);
    await conn.query("DELETE FROM tx_orders WHERE tx_id = ?", [txId]);

    if (oldOrderIds.length > 0) {
      await conn.query(
        `DELETE FROM orders WHERE id IN (${oldOrderIds.map(() => "?").join(",")})`,
        oldOrderIds
      );
    }

    await conn.query(
      `UPDATE tx
       SET tx_date = ?, box_m3 = ?, note = ?, status = 'DRAFT'
       WHERE id = ?`,
      [tx_date, boxM3, note ?? null, txId]
    );

    await conn.query(
      `INSERT INTO tx_legs (tx_id, leg_type, vehicle_id, route_id, dc_id, driver_id, sale_id)
       VALUES (?, 'LINEHAUL', ?, ?, ?, ?, ?)`,
      [
        txId,
        Number(linehaul_vehicle_id),
        Number(route_id),
        Number(dc_id),
        Number(linehaul_driver_id),
        Number(linehaul_sale_id),
      ]
    );

    await conn.query(
      `INSERT INTO tx_legs (tx_id, leg_type, vehicle_id, route_id, dc_id, driver_id, sale_id)
       VALUES (?, 'LASTMILE', ?, NULL, ?, ?, ?)`,
      [
        txId,
        Number(lastmile_vehicle_id),
        Number(dc_id),
        Number(lastmile_driver_id),
        Number(lastmile_sale_id),
      ]
    );

    const createdOrderIds = [];
    let totalM3 = 0;
    let totalQty = 0;

    for (const branch of branches) {
      const [orderResult] = await conn.query(
        "INSERT INTO orders (branch_id, order_date, status) VALUES (?, ?, 'OPEN')",
        [Number(branch.branch_id), tx_date]
      );
      const orderId = Number(orderResult.insertId);
      createdOrderIds.push(orderId);

      for (const item of branch.items) {
        const lotId = Number(item.lot_id);
        const lot = lotMap.get(lotId);
        if (!lot) throw new Error(`lot_id not found: ${lotId}`);
        const pid = Number(lot.product_id);
        const qty = Number(item.qty);
        await conn.query(
          "INSERT INTO order_items (order_id, product_id, lot_id, qty) VALUES (?, ?, ?, ?)",
          [orderId, pid, lotId, qty]
        );
        totalM3 += qty * Number(productMap.get(pid) || 0);
        totalQty += qty;
      }

      await conn.query(
        "INSERT INTO tx_orders (tx_id, order_id, driver_id, sale_id) VALUES (?, ?, ?, ?)",
        [txId, orderId, Number(lastmile_driver_id), Number(lastmile_sale_id)]
      );
    }

    const deltaByLot = buildLotDeltaByLot(requestedByLot, oldReservedByLot);
    for (const [lotId, delta] of deltaByLot.entries()) {

      if (delta > 0) {
        const [u] = await conn.query(
          `UPDATE product_lots
           SET remaining_qty = remaining_qty - ?
           WHERE lot_id = ?
             AND remaining_qty >= ?`,
          [Number(delta), Number(lotId), Number(delta)]
        );
        if (Number(u.affectedRows || 0) !== 1) {
          throw new Error(`insufficient lot stock while reserving lot_id ${lotId}, need_delta ${delta}`);
        }
      } else if (delta < 0) {
        await conn.query(
          "UPDATE product_lots SET remaining_qty = remaining_qty + ? WHERE lot_id = ?",
          [Math.abs(Number(delta)), Number(lotId)]
        );
      }
    }

    await conn.query("UPDATE vehicles SET status = 'IN_USE' WHERE id = ?", [Number(linehaul_vehicle_id)]);
    await conn.query("UPDATE vehicles SET status = 'IN_USE' WHERE id = ?", [Number(lastmile_vehicle_id)]);

    for (const oldVehicleId of oldVehicleIds) {
      const [activeUseRows] = await conn.query(
        `SELECT COUNT(*) AS c
         FROM tx_legs tl
         JOIN tx t ON t.id = tl.tx_id
         WHERE tl.vehicle_id = ?
           AND t.status IN ('DRAFT', 'SUBMITTED', 'IN_PROGRESS')`,
        [Number(oldVehicleId)]
      );
      const activeCount = Number(activeUseRows?.[0]?.c || 0);
      if (activeCount <= 0) {
        await conn.query("UPDATE vehicles SET status = 'AVAILABLE' WHERE id = ?", [Number(oldVehicleId)]);
      }
    }

    await conn.commit();

    return {
      tx_id: txId,
      order_ids: createdOrderIds,
      lastmile_vehicle_id: Number(lastmile_vehicle_id),
      linehaul_driver_id: Number(linehaul_driver_id),
      linehaul_sale_id: Number(linehaul_sale_id),
      lastmile_driver_id: Number(lastmile_driver_id),
      lastmile_sale_id: Number(lastmile_sale_id),
      totals: {
        box_m3: boxM3,
        total_qty: totalQty,
        total_m3: totalM3,
        total_boxes: Math.ceil(totalM3 / boxM3),
      },
    };
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

function formatDateYmd(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function roundPercent(numerator, denominator) {
  if (!(denominator > 0)) return 0;
  const value = (Number(numerator || 0) / Number(denominator || 1)) * 100;
  return Math.round(value * 100) / 100;
}

function mean(values) {
  if (!Array.isArray(values) || values.length === 0) return 0;
  return values.reduce((sum, n) => sum + Number(n || 0), 0) / values.length;
}

function std(values) {
  if (!Array.isArray(values) || values.length < 2) return 0;
  const m = mean(values);
  const variance = values.reduce((sum, n) => sum + (Number(n || 0) - m) ** 2, 0) / (values.length - 1);
  return Math.sqrt(Math.max(variance, 0));
}

function linearSlope(values) {
  if (!Array.isArray(values) || values.length < 2) return 0;
  const n = values.length;
  const xMean = (n - 1) / 2;
  const yMean = mean(values);
  let num = 0;
  let den = 0;
  for (let i = 0; i < n; i += 1) {
    const dx = i - xMean;
    const dy = Number(values[i] || 0) - yMean;
    num += dx * dy;
    den += dx * dx;
  }
  if (den === 0) return 0;
  return num / den;
}

function parseDateYmd(ymd) {
  const [year, month, day] = String(ymd || "").split("-").map((x) => Number(x));
  if (!year || !month || !day) return null;
  return new Date(year, month - 1, day);
}

function forecastDailyQty({ dailySeries = [], days = 7 }) {
  if (!Array.isArray(dailySeries) || dailySeries.length < 4) {
    return {
      horizon_days: 7,
      method: "baseline_moving_avg_trend",
      points: [],
      reason: "insufficient_history",
    };
  }

  const horizonDays = 7;
  const history = dailySeries.map((point) => Number(point.qty || 0));
  const movingWindow = Math.max(3, Math.min(7, Math.floor(days / 2)));
  const tail = history.slice(-movingWindow);
  const baseline = mean(tail);
  const slope = linearSlope(history);
  const residuals = history.map((value, index) => {
    const fitted = baseline + slope * (index - (history.length - movingWindow));
    return Number(value || 0) - fitted;
  });
  const sigma = std(residuals);

  const lastDate = parseDateYmd(dailySeries[dailySeries.length - 1]?.date);
  if (!lastDate) {
    return {
      horizon_days: 7,
      method: "baseline_moving_avg_trend",
      points: [],
      reason: "invalid_history_date",
    };
  }

  const points = [];
  for (let step = 1; step <= horizonDays; step += 1) {
    const date = new Date(lastDate);
    date.setDate(lastDate.getDate() + step);
    const yhat = Math.max(0, baseline + slope * step);
    const lower = Math.max(0, yhat - 1.28 * sigma);
    const upper = Math.max(yhat, yhat + 1.28 * sigma);
    points.push({
      date: formatDateYmd(date),
      yhat: Math.round(yhat * 100) / 100,
      lower: Math.round(lower * 100) / 100,
      upper: Math.round(upper * 100) / 100,
    });
  }

  return {
    horizon_days: horizonDays,
    method: "baseline_moving_avg_trend",
    points,
  };
}

async function getTxInsights({
  days = 7,
  top_n = 5,
  statuses = ["SUBMITTED", "IN_PROGRESS", "COMPLETED"],
  metric = "qty",
  include_forecast = true,
} = {}) {
  const safeDays = Number.isInteger(Number(days)) && Number(days) > 0 ? Number(days) : 7;
  const safeTopN = Number.isInteger(Number(top_n)) && Number(top_n) > 0 ? Number(top_n) : 5;
  const statusList = Array.isArray(statuses) && statuses.length > 0 ? statuses : ["SUBMITTED", "IN_PROGRESS", "COMPLETED"];

  const today = new Date();
  const endDate = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  const startDate = new Date(endDate);
  startDate.setDate(startDate.getDate() - safeDays + 1);
  const fromDate = formatDateYmd(startDate);
  const toDate = formatDateYmd(endDate);

  const statusPlaceholders = statusList.map(() => "?").join(",");
  const baseParams = [fromDate, toDate, ...statusList];

  const [dailyQtyRows] = await pool.query(
    `SELECT
       DATE_FORMAT(t.tx_date, '%Y-%m-%d') AS date,
       COALESCE(SUM(oi.qty), 0) AS qty
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})
     GROUP BY DATE_FORMAT(t.tx_date, '%Y-%m-%d')
     ORDER BY DATE_FORMAT(t.tx_date, '%Y-%m-%d') ASC`,
    baseParams
  );
  const dailyQtySeries = dailyQtyRows.map((row) => ({
    date: row.date,
    qty: Number(row.qty || 0),
  }));

  const [productTotalRows] = await pool.query(
    `SELECT
       COALESCE(SUM(oi.qty), 0) AS total_qty,
       COUNT(DISTINCT o.id) AS total_orders
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})`,
    baseParams
  );

  const productTotals = {
    total_qty: Number(productTotalRows[0]?.total_qty || 0),
    total_orders: Number(productTotalRows[0]?.total_orders || 0),
  };

  const [productTopRows] = await pool.query(
    `SELECT
       p.product_id,
       p.name AS product_name,
       SUM(oi.qty) AS qty,
       COUNT(DISTINCT o.id) AS orders_count,
       COUNT(DISTINCT o.branch_id) AS branches_count
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     JOIN products p ON p.product_id = oi.product_id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})
     GROUP BY p.product_id, p.name
     ORDER BY qty DESC, p.product_id ASC
     LIMIT ?`,
    [...baseParams, safeTopN]
  );

  const productTop = productTopRows.map((row) => ({
    product_id: Number(row.product_id),
    product_name: row.product_name,
    qty: Number(row.qty || 0),
    orders_count: Number(row.orders_count || 0),
    branches_count: Number(row.branches_count || 0),
    share_pct: roundPercent(row.qty, productTotals.total_qty),
  }));

  const [branchTotalRows] = await pool.query(
    `SELECT
       COALESCE(SUM(oi.qty), 0) AS total_qty,
       COUNT(DISTINCT o.id) AS total_orders
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})`,
    baseParams
  );

  const branchTotals = {
    total_qty: Number(branchTotalRows[0]?.total_qty || 0),
    total_orders: Number(branchTotalRows[0]?.total_orders || 0),
  };

  const [branchTopRows] = await pool.query(
    `SELECT
       b.id AS branch_id,
       lb.name AS branch_name,
       d.id AS distributor_id,
       ld.name AS distributor_name,
       SUM(oi.qty) AS qty,
       COUNT(DISTINCT o.id) AS orders_count,
       COUNT(DISTINCT t.id) AS tx_count
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     JOIN branches b ON b.id = o.branch_id
     JOIN locations lb ON lb.id = b.location_id
     JOIN distributors d ON d.id = b.distributor_id
     JOIN locations ld ON ld.id = d.location_id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})
     GROUP BY b.id, lb.name, d.id, ld.name
     ORDER BY qty DESC, b.id ASC
     LIMIT ?`,
    [...baseParams, safeTopN]
  );

  const branchTop = branchTopRows.map((row) => ({
    branch_id: Number(row.branch_id),
    branch_name: row.branch_name,
    distributor_id: Number(row.distributor_id),
    distributor_name: row.distributor_name,
    qty: Number(row.qty || 0),
    orders_count: Number(row.orders_count || 0),
    tx_count: Number(row.tx_count || 0),
    share_pct: roundPercent(row.qty, branchTotals.total_qty),
  }));

  const [distributorTotalRows] = await pool.query(
    `SELECT
       COALESCE(SUM(oi.qty), 0) AS total_qty,
       COUNT(DISTINCT o.id) AS total_orders
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})`,
    baseParams
  );

  const distributorTotals = {
    total_qty: Number(distributorTotalRows[0]?.total_qty || 0),
    total_orders: Number(distributorTotalRows[0]?.total_orders || 0),
  };

  const [distributorTopRows] = await pool.query(
    `SELECT
       d.id AS distributor_id,
       ld.name AS distributor_name,
       SUM(oi.qty) AS qty,
       COUNT(DISTINCT o.id) AS orders_count,
       COUNT(DISTINCT o.branch_id) AS branch_count,
       COUNT(DISTINCT t.id) AS tx_count
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     JOIN branches b ON b.id = o.branch_id
     JOIN distributors d ON d.id = b.distributor_id
     JOIN locations ld ON ld.id = d.location_id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})
     GROUP BY d.id, ld.name
     ORDER BY qty DESC, d.id ASC
     LIMIT ?`,
    [...baseParams, safeTopN]
  );

  const distributorTop = distributorTopRows.map((row) => ({
    distributor_id: Number(row.distributor_id),
    distributor_name: row.distributor_name,
    qty: Number(row.qty || 0),
    orders_count: Number(row.orders_count || 0),
    branch_count: Number(row.branch_count || 0),
    tx_count: Number(row.tx_count || 0),
    share_pct: roundPercent(row.qty, distributorTotals.total_qty),
  }));

  const tops = {
    products: {
      total_qty: productTotals.total_qty,
      total_orders: productTotals.total_orders,
      top: productTop,
    },
    branches: {
      total_qty: branchTotals.total_qty,
      total_orders: branchTotals.total_orders,
      top: branchTop,
    },
    distributors: {
      total_qty: distributorTotals.total_qty,
      total_orders: distributorTotals.total_orders,
      top: distributorTop,
    },
  };

  const response = {
    window: {
      from_date: fromDate,
      to_date: toDate,
      days: safeDays,
      statuses: statusList,
    },
    metric,
    date_basis: "tx_date",
    series: {
      daily_qty: dailyQtySeries,
    },
    tops,
    // keep backward-compatible fields already used by current dashboard
    products: tops.products,
    branches: tops.branches,
    distributors: tops.distributors,
    generated_at: new Date().toISOString(),
  };

  if (include_forecast) {
    response.forecast = forecastDailyQty({ dailySeries: dailyQtySeries, days: safeDays });
  } else {
    response.forecast = {
      horizon_days: 7,
      method: "baseline_moving_avg_trend",
      points: [],
      disabled: true,
    };
  }

  return response;
}

function buildInsightsWindow(days = 7) {
  const safeDays = Number.isInteger(Number(days)) && Number(days) > 0 ? Number(days) : 7;
  const endDate = new Date();
  endDate.setHours(0, 0, 0, 0);
  const startDate = new Date(endDate);
  startDate.setDate(startDate.getDate() - safeDays + 1);

  const prevEndDate = new Date(startDate);
  prevEndDate.setDate(prevEndDate.getDate() - 1);
  const prevStartDate = new Date(prevEndDate);
  prevStartDate.setDate(prevStartDate.getDate() - safeDays + 1);

  return {
    safeDays,
    fromDate: formatDateYmd(startDate),
    toDate: formatDateYmd(endDate),
    prevFromDate: formatDateYmd(prevStartDate),
    prevToDate: formatDateYmd(prevEndDate),
  };
}

async function getTxDashboardOverview({
  days = 7,
  top_n = 5,
  statuses = ["SUBMITTED", "IN_PROGRESS", "COMPLETED"],
  metric = "qty",
} = {}) {
  const safeTopN = Number.isInteger(Number(top_n)) && Number(top_n) > 0 ? Number(top_n) : 5;
  const statusList = Array.isArray(statuses) && statuses.length > 0 ? statuses : ["SUBMITTED", "IN_PROGRESS", "COMPLETED"];
  const { safeDays, fromDate, toDate, prevFromDate, prevToDate } = buildInsightsWindow(days);
  const statusPlaceholders = statusList.map(() => "?").join(",");

  const baseParams = [fromDate, toDate, ...statusList];
  const prevBaseParams = [prevFromDate, prevToDate, ...statusList];

  const [masterCountRows] = await pool.query(
    `SELECT
       (SELECT COUNT(*) FROM products) AS products_count,
       (SELECT COUNT(*) FROM branches) AS branches_count,
       (SELECT COUNT(*) FROM distributors) AS distributors_count`
  );
  const masterCounts = masterCountRows[0] || {};

  const [txCountRows] = await pool.query(
    `SELECT
       COUNT(*) AS trips,
       COALESCE(SUM(CASE WHEN t.status = 'DRAFT' THEN 1 ELSE 0 END), 0) AS draft_trips
     FROM tx t
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})`,
    baseParams
  );
  const trips = Number(txCountRows[0]?.trips || 0);
  const draftTrips = Number(txCountRows[0]?.draft_trips || 0);

  const [metricRows] = await pool.query(
    `SELECT
       COALESCE(SUM(oi.qty), 0) AS total_qty,
       COUNT(DISTINCT o.id) AS total_orders
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})`,
    baseParams
  );
  const totalQty = Number(metricRows[0]?.total_qty || 0);
  const totalOrders = Number(metricRows[0]?.total_orders || 0);

  const [prevTxCountRows] = await pool.query(
    `SELECT COUNT(*) AS trips
     FROM tx t
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})`,
    prevBaseParams
  );
  const prevTrips = Number(prevTxCountRows[0]?.trips || 0);

  const [prevMetricRows] = await pool.query(
    `SELECT COALESCE(SUM(oi.qty), 0) AS total_qty
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})`,
    prevBaseParams
  );
  const prevQty = Number(prevMetricRows[0]?.total_qty || 0);

  const [dailyQtyRows] = await pool.query(
    `SELECT
       DATE_FORMAT(t.tx_date, '%Y-%m-%d') AS date,
       COALESCE(SUM(oi.qty), 0) AS qty
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})
     GROUP BY DATE_FORMAT(t.tx_date, '%Y-%m-%d')
     ORDER BY DATE_FORMAT(t.tx_date, '%Y-%m-%d') ASC`,
    baseParams
  );
  const dailyQtySeries = dailyQtyRows.map((row) => ({
    date: row.date,
    qty: Number(row.qty || 0),
  }));

  const [statusRows] = await pool.query(
    `SELECT t.status, COUNT(*) AS c
     FROM tx t
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})
     GROUP BY t.status
     ORDER BY c DESC, t.status ASC`,
    baseParams
  );
  const statusSplit = statusRows.map((row) => ({
    status: String(row.status || ""),
    count: Number(row.c || 0),
  }));

  const [productTopRows] = await pool.query(
    `SELECT
       p.product_id,
       p.name AS product_name,
       SUM(oi.qty) AS qty
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     JOIN products p ON p.product_id = oi.product_id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})
     GROUP BY p.product_id, p.name
     ORDER BY qty DESC, p.product_id ASC
     LIMIT ?`,
    [...baseParams, safeTopN]
  );

  const [branchTopRows] = await pool.query(
    `SELECT
       b.id AS branch_id,
       lb.name AS branch_name,
       SUM(oi.qty) AS qty
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     JOIN branches b ON b.id = o.branch_id
     JOIN locations lb ON lb.id = b.location_id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})
     GROUP BY b.id, lb.name
     ORDER BY qty DESC, b.id ASC
     LIMIT ?`,
    [...baseParams, safeTopN]
  );

  const [distributorTopRows] = await pool.query(
    `SELECT
       d.id AS distributor_id,
       ld.name AS distributor_name,
       SUM(oi.qty) AS qty
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     JOIN branches b ON b.id = o.branch_id
     JOIN distributors d ON d.id = b.distributor_id
     JOIN locations ld ON ld.id = d.location_id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})
     GROUP BY d.id, ld.name
     ORDER BY qty DESC, d.id ASC
     LIMIT ?`,
    [...baseParams, safeTopN]
  );

  const productsTop = productTopRows.map((row) => ({
    product_id: row.product_id,
    product_name: row.product_name,
    qty: Number(row.qty || 0),
    share_pct: roundPercent(row.qty, totalQty),
  }));
  const branchesTop = branchTopRows.map((row) => ({
    branch_id: Number(row.branch_id),
    branch_name: row.branch_name,
    qty: Number(row.qty || 0),
    share_pct: roundPercent(row.qty, totalQty),
  }));
  const distributorsTop = distributorTopRows.map((row) => ({
    distributor_id: Number(row.distributor_id),
    distributor_name: row.distributor_name,
    qty: Number(row.qty || 0),
    share_pct: roundPercent(row.qty, totalQty),
  }));

  return {
    window: {
      from_date: fromDate,
      to_date: toDate,
      days: safeDays,
      statuses: statusList,
    },
    metric,
    generated_at: new Date().toISOString(),
    kpis: {
      products: Number(masterCounts.products_count || 0),
      branches: Number(masterCounts.branches_count || 0),
      distributors: Number(masterCounts.distributors_count || 0),
      trips,
      draft_trips: draftTrips,
      active_trips: Math.max(0, trips - draftTrips),
      total_qty: totalQty,
      total_orders: totalOrders,
      growth_pct: {
        trips: roundPercent(trips - prevTrips, Math.max(prevTrips, 1)),
        qty: roundPercent(totalQty - prevQty, Math.max(prevQty, 1)),
      },
    },
    series: {
      daily_qty: dailyQtySeries,
    },
    tops: {
      products: { total_qty: totalQty, top: productsTop },
      branches: { total_qty: totalQty, top: branchesTop },
      distributors: { total_qty: totalQty, top: distributorsTop },
    },
    status_split: statusSplit,
  };
}

async function getTxDashboardDrilldown({
  days = 7,
  top_n = 5,
  statuses = ["SUBMITTED", "IN_PROGRESS", "COMPLETED"],
  metric = "qty",
  entity_type,
  entity_id,
} = {}) {
  const safeTopN = Number.isInteger(Number(top_n)) && Number(top_n) > 0 ? Number(top_n) : 5;
  const statusList = Array.isArray(statuses) && statuses.length > 0 ? statuses : ["SUBMITTED", "IN_PROGRESS", "COMPLETED"];
  const { safeDays, fromDate, toDate } = buildInsightsWindow(days);
  const statusPlaceholders = statusList.map(() => "?").join(",");
  const baseParams = [fromDate, toDate, ...statusList];

  const [totalRows] = await pool.query(
    `SELECT COALESCE(SUM(oi.qty), 0) AS total_qty
     FROM tx t
     JOIN tx_orders xo ON xo.tx_id = t.id
     JOIN orders o ON o.id = xo.order_id
     JOIN order_items oi ON oi.order_id = o.id
     WHERE t.tx_date BETWEEN ? AND ?
       AND t.status IN (${statusPlaceholders})`,
    baseParams
  );
  const allQty = Number(totalRows[0]?.total_qty || 0);

  let entity = { id: entity_id, type: entity_type, name: "-" };
  let summary = { qty: 0, orders_count: 0, tx_count: 0, share_pct: 0 };
  let topProducts = [];
  let topBranches = [];
  let dailyQty = [];
  let recentTrips = [];

  if (entity_type === "branch") {
    const [entityRows] = await pool.query(
      `SELECT b.id AS branch_id, lb.name AS branch_name, d.id AS distributor_id, ld.name AS distributor_name
       FROM branches b
       JOIN locations lb ON lb.id = b.location_id
       JOIN distributors d ON d.id = b.distributor_id
       JOIN locations ld ON ld.id = d.location_id
       WHERE b.id = ?
       LIMIT 1`,
      [entity_id]
    );
    const e = entityRows[0] || {};
    entity = {
      id: Number(e.branch_id || entity_id),
      type: "branch",
      name: e.branch_name || `Branch #${entity_id}`,
      distributor_id: Number(e.distributor_id || 0),
      distributor_name: e.distributor_name || "-",
    };

    const [summaryRows] = await pool.query(
      `SELECT
         COALESCE(SUM(oi.qty), 0) AS qty,
         COUNT(DISTINCT o.id) AS orders_count,
         COUNT(DISTINCT t.id) AS tx_count
       FROM tx t
       JOIN tx_orders xo ON xo.tx_id = t.id
       JOIN orders o ON o.id = xo.order_id
       JOIN order_items oi ON oi.order_id = o.id
       WHERE t.tx_date BETWEEN ? AND ?
         AND t.status IN (${statusPlaceholders})
         AND o.branch_id = ?`,
      [...baseParams, entity_id]
    );
    summary = {
      qty: Number(summaryRows[0]?.qty || 0),
      orders_count: Number(summaryRows[0]?.orders_count || 0),
      tx_count: Number(summaryRows[0]?.tx_count || 0),
      share_pct: roundPercent(summaryRows[0]?.qty || 0, allQty),
    };

    const [topProductRows] = await pool.query(
      `SELECT
         p.product_id,
         p.name AS product_name,
         SUM(oi.qty) AS qty,
         COUNT(DISTINCT o.id) AS orders_count
       FROM tx t
       JOIN tx_orders xo ON xo.tx_id = t.id
       JOIN orders o ON o.id = xo.order_id
       JOIN order_items oi ON oi.order_id = o.id
       JOIN products p ON p.product_id = oi.product_id
       WHERE t.tx_date BETWEEN ? AND ?
         AND t.status IN (${statusPlaceholders})
         AND o.branch_id = ?
       GROUP BY p.product_id, p.name
       ORDER BY qty DESC, p.product_id ASC
       LIMIT ?`,
      [...baseParams, entity_id, safeTopN]
    );
    topProducts = topProductRows.map((row) => ({
      product_id: row.product_id,
      product_name: row.product_name,
      qty: Number(row.qty || 0),
      orders_count: Number(row.orders_count || 0),
      share_pct: roundPercent(row.qty, summary.qty),
    }));

    const [dailyRows] = await pool.query(
      `SELECT DATE_FORMAT(t.tx_date, '%Y-%m-%d') AS date, COALESCE(SUM(oi.qty), 0) AS qty
       FROM tx t
       JOIN tx_orders xo ON xo.tx_id = t.id
       JOIN orders o ON o.id = xo.order_id
       JOIN order_items oi ON oi.order_id = o.id
       WHERE t.tx_date BETWEEN ? AND ?
         AND t.status IN (${statusPlaceholders})
         AND o.branch_id = ?
       GROUP BY DATE_FORMAT(t.tx_date, '%Y-%m-%d')
       ORDER BY DATE_FORMAT(t.tx_date, '%Y-%m-%d') ASC`,
      [...baseParams, entity_id]
    );
    dailyQty = dailyRows.map((row) => ({ date: row.date, qty: Number(row.qty || 0) }));

    const [tripRows] = await pool.query(
      `SELECT
         t.id AS tx_id,
         DATE_FORMAT(MIN(t.tx_date), '%Y-%m-%d') AS tx_date,
         t.status,
         COALESCE(SUM(oi.qty), 0) AS qty,
         COUNT(DISTINCT o.id) AS orders_count
       FROM tx t
       JOIN tx_orders xo ON xo.tx_id = t.id
       JOIN orders o ON o.id = xo.order_id
       JOIN order_items oi ON oi.order_id = o.id
       WHERE t.tx_date BETWEEN ? AND ?
         AND t.status IN (${statusPlaceholders})
         AND o.branch_id = ?
       GROUP BY t.id, t.status
       ORDER BY t.id DESC
       LIMIT 10`,
      [...baseParams, entity_id]
    );
    recentTrips = tripRows.map((row) => ({
      tx_id: Number(row.tx_id),
      tx_date: row.tx_date,
      status: row.status,
      qty: Number(row.qty || 0),
      orders_count: Number(row.orders_count || 0),
    }));
  } else if (entity_type === "distributor") {
    const [entityRows] = await pool.query(
      `SELECT d.id AS distributor_id, ld.name AS distributor_name
       FROM distributors d
       JOIN locations ld ON ld.id = d.location_id
       WHERE d.id = ?
       LIMIT 1`,
      [entity_id]
    );
    const e = entityRows[0] || {};
    entity = {
      id: Number(e.distributor_id || entity_id),
      type: "distributor",
      name: e.distributor_name || `Distributor #${entity_id}`,
    };

    const [summaryRows] = await pool.query(
      `SELECT
         COALESCE(SUM(oi.qty), 0) AS qty,
         COUNT(DISTINCT o.id) AS orders_count,
         COUNT(DISTINCT t.id) AS tx_count
       FROM tx t
       JOIN tx_orders xo ON xo.tx_id = t.id
       JOIN orders o ON o.id = xo.order_id
       JOIN order_items oi ON oi.order_id = o.id
       JOIN branches b ON b.id = o.branch_id
       WHERE t.tx_date BETWEEN ? AND ?
         AND t.status IN (${statusPlaceholders})
         AND b.distributor_id = ?`,
      [...baseParams, entity_id]
    );
    summary = {
      qty: Number(summaryRows[0]?.qty || 0),
      orders_count: Number(summaryRows[0]?.orders_count || 0),
      tx_count: Number(summaryRows[0]?.tx_count || 0),
      share_pct: roundPercent(summaryRows[0]?.qty || 0, allQty),
    };

    const [topProductRows] = await pool.query(
      `SELECT
         p.product_id,
         p.name AS product_name,
         SUM(oi.qty) AS qty,
         COUNT(DISTINCT o.id) AS orders_count
       FROM tx t
       JOIN tx_orders xo ON xo.tx_id = t.id
       JOIN orders o ON o.id = xo.order_id
       JOIN order_items oi ON oi.order_id = o.id
       JOIN products p ON p.product_id = oi.product_id
       JOIN branches b ON b.id = o.branch_id
       WHERE t.tx_date BETWEEN ? AND ?
         AND t.status IN (${statusPlaceholders})
         AND b.distributor_id = ?
       GROUP BY p.product_id, p.name
       ORDER BY qty DESC, p.product_id ASC
       LIMIT ?`,
      [...baseParams, entity_id, safeTopN]
    );
    topProducts = topProductRows.map((row) => ({
      product_id: row.product_id,
      product_name: row.product_name,
      qty: Number(row.qty || 0),
      orders_count: Number(row.orders_count || 0),
      share_pct: roundPercent(row.qty, summary.qty),
    }));

    const [dailyRows] = await pool.query(
      `SELECT DATE_FORMAT(t.tx_date, '%Y-%m-%d') AS date, COALESCE(SUM(oi.qty), 0) AS qty
       FROM tx t
       JOIN tx_orders xo ON xo.tx_id = t.id
       JOIN orders o ON o.id = xo.order_id
       JOIN order_items oi ON oi.order_id = o.id
       JOIN branches b ON b.id = o.branch_id
       WHERE t.tx_date BETWEEN ? AND ?
         AND t.status IN (${statusPlaceholders})
         AND b.distributor_id = ?
       GROUP BY DATE_FORMAT(t.tx_date, '%Y-%m-%d')
       ORDER BY DATE_FORMAT(t.tx_date, '%Y-%m-%d') ASC`,
      [...baseParams, entity_id]
    );
    dailyQty = dailyRows.map((row) => ({ date: row.date, qty: Number(row.qty || 0) }));

    const [tripRows] = await pool.query(
      `SELECT
         t.id AS tx_id,
         DATE_FORMAT(MIN(t.tx_date), '%Y-%m-%d') AS tx_date,
         t.status,
         COALESCE(SUM(oi.qty), 0) AS qty,
         COUNT(DISTINCT o.id) AS orders_count
       FROM tx t
       JOIN tx_orders xo ON xo.tx_id = t.id
       JOIN orders o ON o.id = xo.order_id
       JOIN order_items oi ON oi.order_id = o.id
       JOIN branches b ON b.id = o.branch_id
       WHERE t.tx_date BETWEEN ? AND ?
         AND t.status IN (${statusPlaceholders})
         AND b.distributor_id = ?
       GROUP BY t.id, t.status
       ORDER BY t.id DESC
       LIMIT 10`,
      [...baseParams, entity_id]
    );
    recentTrips = tripRows.map((row) => ({
      tx_id: Number(row.tx_id),
      tx_date: row.tx_date,
      status: row.status,
      qty: Number(row.qty || 0),
      orders_count: Number(row.orders_count || 0),
    }));
  } else if (entity_type === "product") {
    const [entityRows] = await pool.query(
      `SELECT p.product_id, p.name AS product_name
       FROM products p
       WHERE p.product_id = ?
       LIMIT 1`,
      [entity_id]
    );
    const e = entityRows[0] || {};
    entity = {
      id: e.product_id || entity_id,
      type: "product",
      name: e.product_name || `Product #${entity_id}`,
    };

    const [summaryRows] = await pool.query(
      `SELECT
         COALESCE(SUM(oi.qty), 0) AS qty,
         COUNT(DISTINCT o.id) AS orders_count,
         COUNT(DISTINCT t.id) AS tx_count
       FROM tx t
       JOIN tx_orders xo ON xo.tx_id = t.id
       JOIN orders o ON o.id = xo.order_id
       JOIN order_items oi ON oi.order_id = o.id
       WHERE t.tx_date BETWEEN ? AND ?
         AND t.status IN (${statusPlaceholders})
         AND oi.product_id = ?`,
      [...baseParams, entity_id]
    );
    summary = {
      qty: Number(summaryRows[0]?.qty || 0),
      orders_count: Number(summaryRows[0]?.orders_count || 0),
      tx_count: Number(summaryRows[0]?.tx_count || 0),
      share_pct: roundPercent(summaryRows[0]?.qty || 0, allQty),
    };

    const [topBranchRows] = await pool.query(
      `SELECT
         b.id AS branch_id,
         lb.name AS branch_name,
         SUM(oi.qty) AS qty,
         COUNT(DISTINCT o.id) AS orders_count
       FROM tx t
       JOIN tx_orders xo ON xo.tx_id = t.id
       JOIN orders o ON o.id = xo.order_id
       JOIN order_items oi ON oi.order_id = o.id
       JOIN branches b ON b.id = o.branch_id
       JOIN locations lb ON lb.id = b.location_id
       WHERE t.tx_date BETWEEN ? AND ?
         AND t.status IN (${statusPlaceholders})
         AND oi.product_id = ?
       GROUP BY b.id, lb.name
       ORDER BY qty DESC, b.id ASC
       LIMIT ?`,
      [...baseParams, entity_id, safeTopN]
    );
    topBranches = topBranchRows.map((row) => ({
      branch_id: Number(row.branch_id),
      branch_name: row.branch_name,
      qty: Number(row.qty || 0),
      orders_count: Number(row.orders_count || 0),
      share_pct: roundPercent(row.qty, summary.qty),
    }));

    const [dailyRows] = await pool.query(
      `SELECT DATE_FORMAT(t.tx_date, '%Y-%m-%d') AS date, COALESCE(SUM(oi.qty), 0) AS qty
       FROM tx t
       JOIN tx_orders xo ON xo.tx_id = t.id
       JOIN orders o ON o.id = xo.order_id
       JOIN order_items oi ON oi.order_id = o.id
       WHERE t.tx_date BETWEEN ? AND ?
         AND t.status IN (${statusPlaceholders})
         AND oi.product_id = ?
       GROUP BY DATE_FORMAT(t.tx_date, '%Y-%m-%d')
       ORDER BY DATE_FORMAT(t.tx_date, '%Y-%m-%d') ASC`,
      [...baseParams, entity_id]
    );
    dailyQty = dailyRows.map((row) => ({ date: row.date, qty: Number(row.qty || 0) }));

    const [tripRows] = await pool.query(
      `SELECT
         t.id AS tx_id,
         DATE_FORMAT(MIN(t.tx_date), '%Y-%m-%d') AS tx_date,
         t.status,
         COALESCE(SUM(oi.qty), 0) AS qty,
         COUNT(DISTINCT o.id) AS orders_count
       FROM tx t
       JOIN tx_orders xo ON xo.tx_id = t.id
       JOIN orders o ON o.id = xo.order_id
       JOIN order_items oi ON oi.order_id = o.id
       WHERE t.tx_date BETWEEN ? AND ?
         AND t.status IN (${statusPlaceholders})
         AND oi.product_id = ?
       GROUP BY t.id, t.status
       ORDER BY t.id DESC
       LIMIT 10`,
      [...baseParams, entity_id]
    );
    recentTrips = tripRows.map((row) => ({
      tx_id: Number(row.tx_id),
      tx_date: row.tx_date,
      status: row.status,
      qty: Number(row.qty || 0),
      orders_count: Number(row.orders_count || 0),
    }));
  }

  return {
    window: {
      from_date: fromDate,
      to_date: toDate,
      days: safeDays,
      statuses: statusList,
    },
    metric,
    generated_at: new Date().toISOString(),
    entity,
    summary,
    top_products: topProducts,
    top_branches: topBranches,
    series: {
      daily_qty: dailyQty,
    },
    recent_trips: recentTrips,
  };
}

// summary: total_volume_m3, boxes (ceil), returns_volume, per-leg capacity check
async function getTxSummary(txId) {
  const txRow = await getTxById(txId);
  const boxM3 = Number(txRow.box_m3 || 0.036);

  // total OUT volume from orders
  const [outRows] = await pool.query(
    `SELECT
       COALESCE(SUM(oi.qty * p.volume_m3), 0) AS total_out_m3
     FROM tx_orders xo
     JOIN order_items oi ON oi.order_id = xo.order_id
     JOIN products p ON p.product_id = oi.product_id
     WHERE xo.tx_id = ?`,
    [txId]
  );

  // total IN volume from returns
  const [inRows] = await pool.query(
    `SELECT
       COALESCE(SUM(r.qty * p.volume_m3), 0) AS total_in_m3
     FROM tx_returns r
     JOIN products p ON p.product_id = r.product_id
     WHERE r.tx_id = ?`,
    [txId]
  );

  const totalOutM3 = Number(outRows[0]?.total_out_m3 || 0);
  const totalInM3 = Number(inRows[0]?.total_in_m3 || 0);

  const totalOutBoxes = Math.ceil(totalOutM3 / boxM3);
  const totalInBoxes = Math.ceil(totalInM3 / boxM3);

  // legs capacity check
  const [legs] = await pool.query(
    `SELECT l.id, l.leg_type, l.vehicle_id, v.capacity_boxes
     FROM tx_legs l
     JOIN vehicles v ON v.id = l.vehicle_id
     WHERE l.tx_id = ?
     ORDER BY l.id`,
    [txId]
  );

  // สำหรับตอนนี้ เราใช้ "OUT boxes" เป็นโหลดหลักของรถส่ง
  const legChecks = legs.map((l) => ({
    leg_id: l.id,
    leg_type: l.leg_type,
    vehicle_id: l.vehicle_id,
    capacity_boxes: Number(l.capacity_boxes || 0),
    used_boxes_estimate: totalOutBoxes,
    over_capacity: totalOutBoxes > Number(l.capacity_boxes || 0),
  }));

  return {
    tx: txRow,
    totals: {
      box_m3: boxM3,
      total_out_m3: totalOutM3,
      total_out_boxes: totalOutBoxes,
      total_in_m3: totalInM3,
      total_in_boxes: totalInBoxes,
    },
    legs: legChecks,
  };
}

async function findMissingBranches(branchIds) {
  if (branchIds.length === 0) return [];
  const [rows] = await pool.query(
    `SELECT id FROM branches WHERE id IN (${branchIds.map(() => "?").join(",")})`,
    branchIds
  );
  const ok = new Set(rows.map((r) => Number(r.id)));
  return branchIds.filter((id) => !ok.has(Number(id)));
}

async function getOpenOrderIdsByBranches(branchIds) {
  if (branchIds.length === 0) return [];
  const [rows] = await pool.query(
    `SELECT id
     FROM orders
     WHERE status = 'OPEN'
       AND branch_id IN (${branchIds.map(() => "?").join(",")})
     ORDER BY id`,
    branchIds
  );
  return rows.map((r) => Number(r.id));
}

async function getOpenOrderCountsByBranches(branchIds) {
  if (branchIds.length === 0) return [];

  const [rows] = await pool.query(
    `SELECT branch_id, COUNT(*) AS open_orders
     FROM orders
     WHERE status = 'OPEN'
       AND branch_id IN (${branchIds.map(() => "?").join(",")})
     GROUP BY branch_id
     ORDER BY branch_id`,
    branchIds
  );

  // ให้ครบทุก branch ที่ส่งมา (branch ที่ไม่มี OPEN ก็ = 0)
  const map = new Map(rows.map(r => [Number(r.branch_id), Number(r.open_orders)]));
  return branchIds.map(bid => ({
    branch_id: Number(bid),
    open_orders: map.get(Number(bid)) ?? 0,
  }));
}

async function countOrdersByBranch(orderIds) {
  if (!orderIds || orderIds.length === 0) return [];
  const [rows] = await pool.query(
    `SELECT branch_id, COUNT(*) AS c
     FROM orders
     WHERE id IN (${orderIds.map(() => "?").join(",")})
     GROUP BY branch_id`,
    orderIds
  );
  return rows;
}

async function getTxSelected(txId) {
  const txRow = await getTxById(txId);
  const boxM3 = Number(txRow.box_m3 || 0.036);
  const [legs] = await pool.query(
    `SELECT leg_type, vehicle_id, route_id, dc_id, driver_id, sale_id
     FROM tx_legs
     WHERE tx_id = ?
     ORDER BY FIELD(leg_type, 'LINEHAUL', 'LASTMILE'), id`, 
    [txId]
  );

  // 1) orders ที่ถูกผูกใน tx (ดึง branch + จำนวนรายการ + ปริมาตรรวมต่อ order)
  const [orders] = await pool.query(
    `SELECT
       o.id AS order_id,
       o.branch_id,
       o.order_date,
       o.status,
       COUNT(oi.id) AS item_lines,
       COALESCE(SUM(oi.qty * p.volume_m3), 0) AS order_volume_m3
     FROM tx_orders xo
     JOIN orders o ON o.id = xo.order_id
     LEFT JOIN order_items oi ON oi.order_id = o.id
     LEFT JOIN products p ON p.product_id = oi.product_id
     WHERE xo.tx_id = ?
     GROUP BY o.id, o.branch_id, o.order_date, o.status
     ORDER BY o.branch_id, o.id`,
    [txId]
  );

  // 2) branch list ที่เกี่ยวข้อง (จาก orders ใน tx)
  const [branchesFromOrders] = await pool.query(
    `SELECT DISTINCT
       b.id AS branch_id,
       b.distributor_id,
       b.location_id,
       l.name AS branch_name,
       l.lat,
       l.lng
     FROM tx_orders xo
     JOIN orders o ON o.id = xo.order_id
     JOIN branches b ON b.id = o.branch_id
     JOIN locations l ON l.id = b.location_id
     WHERE xo.tx_id = ?
     ORDER BY b.id`,
    [txId]
  );

  // 3) returns list (ถ้ามี)
  const [returns] = await pool.query(
    `SELECT
       r.id AS return_id,
       r.branch_id,
       lb.name AS branch_name,
       r.product_id,
       p.name AS product_name,
       r.qty,
       r.lot_code,
       r.expiry_date,
       (r.qty * p.volume_m3) AS volume_m3
     FROM tx_returns r
     JOIN products p ON p.product_id = r.product_id
     LEFT JOIN branches b ON b.id = r.branch_id
     LEFT JOIN locations lb ON lb.id = b.location_id
     WHERE r.tx_id = ?
     ORDER BY r.branch_id, r.id`,
    [txId]
  );

  // 4) total volume/boxes realtime
  let totalOutM3 = 0;
  let totalInM3 = 0;
  let totalOutBoxes = 0;
  let totalInBoxes = 0;

  // 5) branches ที่เกี่ยวข้อง “รวม returns ด้วย” (กันเคสคืนของจาก branch ที่ไม่มี order ใน tx)
  // (จะรวมแบบง่ายใน memory)
  const branchMap = new Map();
  for (const b of branchesFromOrders) branchMap.set(Number(b.branch_id), b);

  for (const r of returns) {
    const bid = Number(r.branch_id);
    if (!branchMap.has(bid)) {
      // ถ้าไม่มี info branch (กรณี branch ถูกลบหรือ join ไม่ได้) ก็ใส่ขั้นต่ำ
      branchMap.set(bid, {
        branch_id: bid,
        distributor_id: null,
        location_id: null,
        branch_name: r.branch_name ?? null,
        lat: null,
        lng: null,
      });
    }
  }

  const branches = Array.from(branchMap.values()).sort((a, b) => Number(a.branch_id) - Number(b.branch_id));

    // 1.1) ดึง order items ทั้งหมดของ orders ที่อยู่ใน tx (แตกเป็นรายการ)
  const orderIds = orders.map((o) => Number(o.order_id));
  let itemsByOrder = new Map();

  if (orderIds.length > 0) {
    const [orderItems] = await pool.query(
      `SELECT
         oi.order_id,
         oi.product_id,
         oi.lot_id,
         pl.lot_number,
         pl.expiry_date,
         p.name AS product_name,
         oi.qty,
         p.volume_m3,
         (oi.qty * p.volume_m3) AS line_volume_m3
       FROM order_items oi
       JOIN products p ON p.product_id = oi.product_id
       LEFT JOIN product_lots pl ON pl.lot_id = oi.lot_id
       WHERE oi.order_id IN (${orderIds.map(() => "?").join(",")})
       ORDER BY oi.order_id, oi.product_id, oi.lot_id`,
      orderIds
    );

    // group by order_id
    for (const it of orderItems) {
      const oid = Number(it.order_id);
      if (!itemsByOrder.has(oid)) itemsByOrder.set(oid, []);
      itemsByOrder.get(oid).push({
        product_id: it.product_id,
        lot_id: it.lot_id,
        lot_number: it.lot_number,
        expiry_date: it.expiry_date,
        product_name: it.product_name,
        qty: Number(it.qty),
        volume_m3: Number(it.volume_m3),
        line_volume_m3: Number(it.line_volume_m3),
      });
    }
  }

  // 1.2) ใส่ items ลงใน orders[] และคำนวณ order_volume_m3 ใหม่จาก items (กัน null)
  const ordersWithItems = orders.map((o) => {
    const oid = Number(o.order_id);
    const items = itemsByOrder.get(oid) ?? [];

    const calcVol = items.reduce((s, x) => s + Number(x.line_volume_m3 || 0), 0);

    return {
      order_id: Number(o.order_id),
      branch_id: Number(o.branch_id),
      order_date: o.order_date,
      status: o.status,
      item_lines: Number(o.item_lines || items.length),
      order_volume_m3: Number(o.order_volume_m3 ?? calcVol),
      items,
    };
  });

  totalOutM3 = ordersWithItems.reduce((s, o) => s + Number(o.order_volume_m3 || 0), 0);
  totalInM3 = returns.reduce((s, r) => s + Number(r.volume_m3 || 0), 0);
  totalOutBoxes = Math.ceil(totalOutM3 / boxM3);
  totalInBoxes = Math.ceil(totalInM3 / boxM3);
  
  return {
    tx: txRow,
    legs: legs || [],
    branches,      // ✅ branch list ที่เกี่ยวข้อง
    orders: ordersWithItems,        // ✅ orders ที่ถูกผูกใน tx (มี order_volume_m3 ให้)
    returns,       // ✅ รายการรับกลับ (optional)
    totals: {
      box_m3: boxM3,
      total_out_m3: totalOutM3,
      total_out_boxes: totalOutBoxes,
      total_in_m3: totalInM3,
      total_in_boxes: totalInBoxes,
    },
  };
}

module.exports = {
  listTx,
  getTxInsights,
  getTxDashboardOverview,
  getTxDashboardDrilldown,
  txExists,
  vehicleExists,
  routeExists,
  distributorExists,
  branchExists,
  productExists,
  getProductLotById,
  createTx,
  getTxById,
  updateTxDraft,
  updateTxStatus,
  addTxLeg,
  findInvalidOrders,
  addOrdersToTx,
  getOpenOrderIdsByBranch,
  findMissingBranches,
  getOpenOrderIdsByBranches,
  getOpenOrderCountsByBranches,
  countOrdersByBranch,
  addTxReturns,
  previewOrderPlan,
  aiArrangeBranch,
  debugPackBranchItems,
  debugCompareExistingOrder,
  createFromOrderPlan,
  replaceDraftFromOrderPlan,
  getTxSummary,
  getTxSelected,
  __testables: {
    validateEditableLotAvailability,
    buildLotDeltaByLot,
  },
};

