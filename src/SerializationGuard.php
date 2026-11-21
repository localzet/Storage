<?php

/**
 * @package     Localzet Storage
 * @link        https://github.com/localzet/Storage
 *
 * @author      Ivan Zorin <creator@localzet.com>
 * @copyright   Copyright © 2016-2026 Localzet Group. All rights reserved.
 * @license     https://www.gnu.org/licenses/agpl-3.0 GNU Affero General Public License v3.0
 *
 *              This program is free software: you can redistribute it and/or modify
 *              it under the terms of the GNU Affero General Public License as published
 *              by the Free Software Foundation, either version 3 of the License, or
 *              (at your option) any later version.
 *
 *              This program is distributed in the hope that it will be useful,
 *              but WITHOUT ANY WARRANTY; without even the implied warranty of
 *              MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *              GNU Affero General Public License for more details.
 *
 *              You should have received a copy of the GNU Affero General Public License
 *              along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
 *              For any questions, please contact <creator@localzet.com>
 */

namespace localzet\Storage;

/** @internal */
final class SerializationGuard
{
    private const MAX_ARRAY_DEPTH = 128;

    private const MAX_VALUES = 100_000;

    private const MAX_REQUEST_FIELDS = 4;

    public static function supports(
        mixed $value,
        int $maxValues = self::MAX_VALUES,
        int $maxArrayDepth = self::MAX_ARRAY_DEPTH,
    ): bool {
        $activeReferences = [];
        $visitedValues = 0;

        return self::supportsValue($value, null, $activeReferences, $visitedValues, 0, $maxValues, $maxArrayDepth);
    }

    public static function supportsSerializedValue(string $serialized): bool
    {
        return self::supportsSerialized($serialized, self::MAX_VALUES, self::MAX_ARRAY_DEPTH, false);
    }

    public static function supportsRequest(mixed $value): bool
    {
        $requestFields = is_array($value) ? min(count($value), self::MAX_REQUEST_FIELDS) : 0;

        return self::supports($value, self::MAX_VALUES + $requestFields, self::MAX_ARRAY_DEPTH + 1);
    }

    public static function supportsSerializedRequest(string $serialized): bool
    {
        return self::supportsSerialized(
            $serialized,
            self::MAX_VALUES,
            self::MAX_ARRAY_DEPTH + 1,
            true,
        );
    }

    public static function decode(string $serialized, bool $allowObjects): mixed
    {
        if ($allowObjects) {
            return unserialize($serialized, ['allowed_classes' => true]);
        }

        set_error_handler(static function (int $severity, string $message, string $file, int $line): bool {
            if ((error_reporting() & $severity) === 0) {
                return false;
            }

            throw new \ErrorException($message, 0, $severity, $file, $line);
        });

        try {
            return unserialize($serialized, ['allowed_classes' => false]);
        } finally {
            restore_error_handler();
        }
    }

    private static function supportsSerialized(
        string $serialized,
        int $maxValues,
        int $maxArrayDepth,
        bool $isRequest,
    ): bool {
        $offset = 0;
        $visitedValues = 0;

        return self::scanSerializedValue(
            $serialized,
            $offset,
            $visitedValues,
            $maxValues,
            $maxArrayDepth,
            0,
            false,
            $isRequest,
        ) && $offset === strlen($serialized);
    }

    /**
     * @param array<string, true> $activeReferences
     */
    private static function supportsValue(
        mixed $value,
        ?string $reference,
        array &$activeReferences,
        int &$visitedValues,
        int $arrayDepth,
        int $maxValues,
        int $maxArrayDepth,
    ): bool {
        if ($reference !== null) {
            if (isset($activeReferences[$reference])) {
                return false;
            }

            $activeReferences[$reference] = true;
        }

        try {
            if (is_object($value)) {
                return false;
            }
            if (!is_array($value)) {
                return is_scalar($value) || $value === null;
            }

            if ($arrayDepth >= $maxArrayDepth) {
                return false;
            }

            $childDepth = $arrayDepth + 1;
            foreach ($value as $key => $child) {
                if (++$visitedValues > $maxValues) {
                    return false;
                }

                $elementReference = \ReflectionReference::fromArrayElement($value, $key);
                $referenceId = $elementReference === null
                    ? null
                    : bin2hex($elementReference->getId());

                if (!self::supportsValue(
                    $child,
                    $referenceId,
                    $activeReferences,
                    $visitedValues,
                    $childDepth,
                    $maxValues,
                    $maxArrayDepth,
                )) {
                    return false;
                }
            }

            return true;
        } finally {
            if ($reference !== null) {
                unset($activeReferences[$reference]);
            }
        }
    }

    private static function scanSerializedValue(
        string $serialized,
        int &$offset,
        int &$visitedValues,
        int $maxValues,
        int $maxArrayDepth,
        int $arrayDepth,
        bool $isArrayValue,
        bool $isRequest,
    ): bool {
        if ($isArrayValue && ++$visitedValues > $maxValues) {
            return false;
        }

        if ($offset >= strlen($serialized)) {
            return false;
        }

        $type = $serialized[$offset++];
        if ($type === 'N') {
            return self::consume($serialized, $offset, ';');
        }
        if ($type === 'b' || $type === 'i' || $type === 'd') {
            return self::scanScalar($serialized, $offset, $type);
        }
        if ($type === 's') {
            return self::scanString($serialized, $offset);
        }
        if ($type === 'R') {
            if (!self::consume($serialized, $offset, ':')) {
                return false;
            }

            return self::readUnsigned($serialized, $offset, ';') !== null;
        }
        if ($type !== 'a' || $arrayDepth >= $maxArrayDepth) {
            return false;
        }
        if (!self::consume($serialized, $offset, ':')) {
            return false;
        }

        $elementCount = self::readUnsigned($serialized, $offset, ':');
        if ($isRequest && $arrayDepth === 0) {
            $maxValues += min($elementCount ?? 0, self::MAX_REQUEST_FIELDS);
        }
        if ($elementCount === null || $elementCount > $maxValues - $visitedValues) {
            return false;
        }
        if (!self::consume($serialized, $offset, '{')) {
            return false;
        }

        $childDepth = $arrayDepth + 1;
        for ($index = 0; $index < $elementCount; ++$index) {
            if (!self::scanArrayKey($serialized, $offset)
                || !self::scanSerializedValue(
                    $serialized,
                    $offset,
                    $visitedValues,
                    $maxValues,
                    $maxArrayDepth,
                    $childDepth,
                    true,
                    false,
                )
            ) {
                return false;
            }
        }

        return self::consume($serialized, $offset, '}');
    }

    private static function scanArrayKey(string $serialized, int &$offset): bool
    {
        if ($offset >= strlen($serialized)) {
            return false;
        }

        $type = $serialized[$offset++];
        if ($type === 'i') {
            return self::scanScalar($serialized, $offset, 'i');
        }
        if ($type === 's') {
            return self::scanString($serialized, $offset);
        }

        return false;
    }

    private static function scanScalar(string $serialized, int &$offset, string $type): bool
    {
        if (!self::consume($serialized, $offset, ':')) {
            return false;
        }

        $end = strpos($serialized, ';', $offset);
        if ($end === false || $end === $offset || $end - $offset > 64) {
            return false;
        }

        $value = substr($serialized, $offset, $end - $offset);
        $valid = match ($type) {
            'b' => $value === '0' || $value === '1',
            'i' => preg_match('/^-?(?:0|[1-9][0-9]*)$/D', $value) === 1,
            'd' => self::isFloatToken($value),
            default => false,
        };
        if (!$valid) {
            return false;
        }

        $offset = $end + 1;

        return true;
    }

    private static function isFloatToken(string $value): bool
    {
        return in_array($value, ['NAN', 'INF', '-INF'], true)
            || preg_match('/^[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[Ee][+-]?[0-9]+)?$/D', $value) === 1;
    }

    private static function scanString(string $serialized, int &$offset): bool
    {
        if (!self::consume($serialized, $offset, ':')) {
            return false;
        }

        $byteLength = self::readUnsigned($serialized, $offset, ':');
        if ($byteLength === null || !self::consume($serialized, $offset, '"')) {
            return false;
        }

        $remainingBytes = strlen($serialized) - $offset;
        if ($remainingBytes < 2 || $byteLength > $remainingBytes - 2) {
            return false;
        }

        $offset += $byteLength;

        return self::consume($serialized, $offset, '"')
            && self::consume($serialized, $offset, ';');
    }

    private static function readUnsigned(string $serialized, int &$offset, string $terminator): ?int
    {
        $length = strlen($serialized);
        $value = 0;
        $digits = 0;

        while ($offset < $length) {
            $digit = ord($serialized[$offset]) - 48;
            if ($digit < 0 || $digit > 9) {
                break;
            }
            if ($value > intdiv(PHP_INT_MAX - $digit, 10)) {
                return null;
            }

            $value = ($value * 10) + $digit;
            ++$digits;
            ++$offset;
        }

        if ($digits === 0 || $offset >= $length || $serialized[$offset] !== $terminator) {
            return null;
        }

        ++$offset;

        return $value;
    }

    private static function consume(string $serialized, int &$offset, string $expected): bool
    {
        if (!isset($serialized[$offset]) || $serialized[$offset] !== $expected) {
            return false;
        }

        ++$offset;

        return true;
    }
}
