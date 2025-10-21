SELECT d1.number, d1.speciality, d1.hospital
FROM doctors d1
JOIN doctors d2
ON d1.hospital = d2.hospital
AND d1.id <> d2.id;
