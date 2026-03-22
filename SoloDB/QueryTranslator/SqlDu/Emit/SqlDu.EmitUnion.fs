module internal SoloDatabase.EmitUnion

/// Union emission is handled directly in EmitSelect.emitSelect via the
/// UnionAllSelect(head, tail) branch of SelectBody.
///
/// This module exists as a namespace anchor per the modular emitter architecture.
/// All UNION ALL emission logic lives in EmitSelect.fs to keep the recursive
/// emitSelect/emitSelectCore cycle in one module.
