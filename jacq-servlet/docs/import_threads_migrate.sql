INSERT INTO `threads` SELECT *, 1 FROM `import_threads`;
INSERT INTO `thread_logs` SELECT * FROM `import_logs`;

DROP TABLE `import_threads`;
DROP TABLE `import_logs`;
