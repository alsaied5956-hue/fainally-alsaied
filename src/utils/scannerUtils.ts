/**
 * src/utils/scannerUtils.ts
 * 
 * Intelligent Barcode & Scanner Input Normalization and Multi-Device Diagnostic Utilities
 * 
 * Solves:
 * 1. Arabic & Persian Numerals (e.g. ١٠٠١ -> 1001)
 * 2. Barcode scanner delimiters (*1001* from Code39 or #1001 prefixes)
 * 3. Carriage returns, newlines, tabs, and hidden zero-width unicode characters
 * 4. Multi-identifier student lookup (Barcode, phone, parent phone, zero-padded code)
 */

import { Student } from "../types";

/**
 * Normalizes any scanned string or typed barcode input:
 * - Converts Eastern Arabic (٠-٩) and Persian (۰-۹) digits to standard ASCII (0-9)
 * - Strips Code39 delimiter asterisks (*1001* -> 1001)
 * - Strips leading '#' symbol (#1001 -> 1001)
 * - Removes hidden unicode markers, newlines, tabs, and trims whitespace
 */
export function normalizeBarcode(input: string | number | null | undefined): string {
  if (input === null || input === undefined) return "";
  let clean = String(input).trim();

  // Strip Code39 asterisks if present e.g. *1001* -> 1001
  if (clean.startsWith("*") && clean.endsWith("*") && clean.length > 2) {
    clean = clean.slice(1, -1).trim();
  }

  // Strip leading hash if present e.g. #1001 -> 1001
  if (clean.startsWith("#")) {
    clean = clean.slice(1).trim();
  }

  // Convert Arabic & Persian numerals to ASCII 0-9
  clean = clean
    .replace(/[٠۰]/g, "0")
    .replace(/[١۱]/g, "1")
    .replace(/[٢۲]/g, "2")
    .replace(/[٣۳]/g, "3")
    .replace(/[٤۴]/g, "4")
    .replace(/[٥۵]/g, "5")
    .replace(/[٦۶]/g, "6")
    .replace(/[٧۷]/g, "7")
    .replace(/[٨۸]/g, "8")
    .replace(/[٩۹]/g, "9");

  // Remove zero-width characters, control characters, tabs, newlines
  clean = clean.replace(/[\u200B-\u200D\uFEFF\r\n\t]/g, "").trim();

  return clean;
}

/**
 * Strips non-digit characters for phone number comparisons
 */
export function normalizePhone(phone: string | number | null | undefined): string {
  if (!phone) return "";
  const normalized = normalizeBarcode(phone);
  return normalized.replace(/\D/g, "");
}

export interface StudentMatchResult {
  student: Student;
  matchType: "exact_barcode" | "unpadded_barcode" | "student_phone" | "parent_phone" | "case_insensitive";
}

/**
 * Fast multi-identifier student lookup:
 * 1. Exact normalized barcode
 * 2. Barcode stripped of leading zeros (e.g. 0100 -> 100)
 * 3. Barcode padded with leading zeros (e.g. 100 -> 0100)
 * 4. Student Phone Number
 * 5. Parent Phone Number
 */
export function findStudentByScannedCode(
  rawInput: string,
  students: Student[],
  studentMap?: Map<string, Student>
): StudentMatchResult | null {
  const clean = normalizeBarcode(rawInput);
  if (!clean) return null;

  // 1. O(1) Fast Map lookup if provided
  if (studentMap && studentMap.has(clean)) {
    return {
      student: studentMap.get(clean)!,
      matchType: "exact_barcode",
    };
  }

  // 2. Direct exact barcode match
  const exact = students.find((s) => normalizeBarcode(s.barcode) === clean);
  if (exact) {
    return {
      student: exact,
      matchType: "exact_barcode",
    };
  }

  // 3. Match without leading zeros (e.g. user scanned "01001" and student barcode is "1001", or vice-versa)
  const unpaddedInput = clean.replace(/^0+/, "");
  if (unpaddedInput && unpaddedInput !== clean) {
    const unpaddedMatch = students.find(
      (s) => normalizeBarcode(s.barcode).replace(/^0+/, "") === unpaddedInput
    );
    if (unpaddedMatch) {
      return {
        student: unpaddedMatch,
        matchType: "unpadded_barcode",
      };
    }
  }

  // Also check if existing student has leading zero stripped
  const studentWithZeroStripped = students.find(
    (s) => normalizeBarcode(s.barcode).replace(/^0+/, "") === clean
  );
  if (studentWithZeroStripped) {
    return {
      student: studentWithZeroStripped,
      matchType: "unpadded_barcode",
    };
  }

  // 4. Match by phone number or parent phone (if input is 9 to 12 digits)
  const digitsOnly = clean.replace(/\D/g, "");
  if (digitsOnly.length >= 8) {
    const phoneMatch = students.find((s) => {
      const p1 = normalizePhone(s.phone);
      const p2 = normalizePhone(s.parentPhone);
      return (p1 && p1 === digitsOnly) || (p2 && p2 === digitsOnly);
    });

    if (phoneMatch) {
      const matchType = normalizePhone(phoneMatch.parentPhone) === digitsOnly
        ? "parent_phone"
        : "student_phone";
      return {
        student: phoneMatch,
        matchType,
      };
    }
  }

  // 5. Case-insensitive alphanumeric match
  const lower = clean.toLowerCase();
  const caseInsensitive = students.find(
    (s) => normalizeBarcode(s.barcode).toLowerCase() === lower
  );
  if (caseInsensitive) {
    return {
      student: caseInsensitive,
      matchType: "case_insensitive",
    };
  }

  return null;
}
