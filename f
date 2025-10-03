--EVENT:  SST REQUEST - CONTEXT:  CLUSTER
$ grep -i "requested state" cl407/error.11407.log 
2025-09-22 22:30:41 0 [Note] WSREP: Member 0.0 (NODE_21407) requested state transfer from '*any*'. Selected 1.0 (NODE_11407)(SYNCED) as donor.
2025-09-22 22:33:28 0 [Note] WSREP: Member 1.0 (NODE_21407) requested state transfer from '*any*'. Selected 0.0 (NODE_11407)(SYNCED) as donor.
2025-09-22 22:40:01 0 [Note] WSREP: Member 1.0 (NODE_21407) requested state transfer from '*any*'. Selected 0.0 (NODE_11407)(SYNCED) as donor.
2025-09-22 22:41:35 0 [Note] WSREP: Member 2.0 (NODE_31407) requested state transfer from '*any*'. Selected 0.0 (NODE_11407)(SYNCED) as donor.
2025-09-23 10:33:41 0 [Note] WSREP: Member 0.0 (NODE_31407) requested state transfer from '*any*'. Selected 1.0 (NODE_11407)(SYNCED) as donor.
2025-09-23 10:35:11 0 [Note] WSREP: Member 0.0 (NODE_31407) requested state transfer from '*any*'. Selected 1.0 (NODE_11407)(SYNCED) as donor.
2025-09-23 17:56:51 0 [Note] WSREP: Member 2.0 (NODE_31407) requested state transfer from '*any*'. Selected 0.0 (NODE_11407)(SYNCED) as donor.
2025-09-25 13:54:32 0 [Note] WSREP: Member 2.0 (NODE_31407) requested state transfer from '*any*'. Selected 1.0 (NODE_21407)(SYNCED) as donor.
2025-09-25 17:17:36 0 [Note] WSREP: Member 2.0 (NODE_31407) requested state transfer from '*any*'. Selected 0.0 (NODE_11407)(SYNCED) as donor.
2025-09-25 17:22:20 0 [Note] WSREP: Member 2.0 (NODE_31407) requested state transfer from '*any*'. Selected 0.0 (NODE_11407)(SYNCED) as donor.
2025-09-25 18:03:49 0 [Note] WSREP: Member 0.0 (NODE_31407) requested state transfer from '*any*'. Selected 2.0 (NODE_21407)(SYNCED) as donor.
2025-09-25 18:05:41 0 [Note] WSREP: Member 2.0 (NODE_31407) requested state transfer from '*any*'. Selected 1.0 (NODE_21407)(SYNCED) as donor.
2025-09-25 18:07:25 0 [Note] WSREP: Member 2.0 (NODE_31407) requested state transfer from '*any*'. Selected 1.0 (NODE_21407)(SYNCED) as donor.
2025-09-29 16:47:09 0 [Note] WSREP: Member 0.0 (NODE_31407) requested state transfer from '*any*'. Selected 1.0 (NODE_11407)(SYNCED) as donor.
2025-09-29 16:47:39 0 [Note] WSREP: Member 2.0 (NODE_21407) requested state transfer from '*any*'. Selected 0.0 (NODE_31407)(SYNCED) as donor.
2025-09-29 17:12:04 0 [Note] WSREP: Member 1.0 (NODE_11407) requested state transfer from '*any*'. Selected 2.0 (NODE_21407)(SYNCED) as donor.
2025-09-29 23:47:39 0 [Note] WSREP: Member 2.0 (NODE_31407) requested state transfer from '*any*'. Selected 1.0 (NODE_21407)(SYNCED) as donor.

--EVENT:  SST START(DONOR) - CONTEXT: LOCAL
$  grep -i "SST started on donor" cl407/error.11407.log 
WSREP_SST: [INFO] mariabackup SST started on donor (20250922 22:30:41.343)
WSREP_SST: [INFO] mariabackup SST started on donor (20250922 22:33:28.821)
WSREP_SST: [INFO] mariabackup SST started on donor (20250922 22:40:01.808)
WSREP_SST: [INFO] mariabackup SST started on donor (20250922 22:41:35.790)
WSREP_SST: [INFO] mariabackup SST started on donor (20250923 10:33:41.605)
WSREP_SST: [INFO] mariabackup SST started on donor (20250923 10:35:11.694)
WSREP_SST: [INFO] mariabackup SST started on donor (20250923 17:56:51.713)
WSREP_SST: [INFO] mariabackup SST started on donor (20250925 17:17:36.716)
WSREP_SST: [INFO] mariabackup SST started on donor (20250925 17:22:21.013)

--EVENT:  SST START(JOINER) - CONTEXT: LOCAL
$  grep -i "SST started on joiner" cl407/error.31407.log 
WSREP_SST: [INFO] mariabackup SST started on joiner (20250925 17:22:20.840)
WSREP_SST: [INFO] mariabackup SST started on joiner (20250925 18:03:48.674)
WSREP_SST: [INFO] mariabackup SST started on joiner (20250925 18:05:41.294)
WSREP_SST: [INFO] mariabackup SST started on joiner (20250925 18:07:25.682)
WSREP_SST: [INFO] mariabackup SST started on joiner (20250929 16:47:09.370)
WSREP_SST: [INFO] mariabackup SST started on joiner (20250929 23:47:38.813)


--EVENT:  SST COMPLETED(DONOR) - CONTEXT: LOCAL
$ grep -i "SST completed on donor" cl407/error.11407.log 
WSREP_SST: [INFO] mariabackup SST completed on donor (20250922 22:40:17.350)
WSREP_SST: [INFO] mariabackup SST completed on donor (20250922 22:41:51.333)
WSREP_SST: [INFO] mariabackup SST completed on donor (20250923 10:33:57.163)
WSREP_SST: [INFO] mariabackup SST completed on donor (20250923 10:35:27.262)
WSREP_SST: [INFO] mariabackup SST completed on donor (20250923 17:57:07.422)
WSREP_SST: [INFO] mariabackup SST completed on donor (20250925 17:22:54.634)

--EVENT:  SST COMPLETED(JOINER) - CONTEXT: LOCAL
$ grep -i "SST completed on joiner" cl407/error.31407.log 
WSREP_SST: [INFO] mariabackup SST completed on joiner (20250925 17:22:55.533)




--EVENT:  SST SUCCEEDED(on joiner) - CONTEXT: LOCAL - INFO: IST
$ grep -i "WSREP: SST succeeded for position " cl407/error.31407.log 
2025-09-25 17:23:02 3 [Note] WSREP: SST succeeded for position a572a681-97f2-11f0-9f63-c7c3a72b2527:1015495
2025-09-25 18:03:53 3 [Note] WSREP: SST succeeded for position a572a681-97f2-11f0-9f63-c7c3a72b2527:1030550
2025-09-25 18:05:43 3 [Note] WSREP: SST succeeded for position a572a681-97f2-11f0-9f63-c7c3a72b2527:1055182
2025-09-25 18:07:29 3 [Note] WSREP: SST succeeded for position a572a681-97f2-11f0-9f63-c7c3a72b2527:1078278
2025-09-29 16:47:18 3 [Note] WSREP: SST succeeded for position a572a681-97f2-11f0-9f63-c7c3a72b2527:1245650
2025-09-29 23:47:45 3 [Note] WSREP: SST succeeded for position a572a681-97f2-11f0-9f63-c7c3a72b2527:1245656


--EVENT:  IST START - CONTEXT:  LOCAL
$ grep "WSREP: Prepared IST receiver for" cl407/error.31407.log 
2025-09-25 17:22:20 2 [Note] WSREP: Prepared IST receiver for 0-998316, listening at: tcp://192.168.178.77:32408
2025-09-25 18:03:49 2 [Note] WSREP: Prepared IST receiver for 1030551-1041405, listening at: tcp://192.168.178.77:32408
2025-09-25 18:05:41 2 [Note] WSREP: Prepared IST receiver for 1055183-1055184, listening at: tcp://192.168.178.77:32408
2025-09-25 18:07:25 2 [Note] WSREP: Prepared IST receiver for 1078279-1127403, listening at: tcp://192.168.178.77:32408
2025-09-29 16:47:09 2 [Note] WSREP: Prepared IST receiver for 1245651-1245652, listening at: tcp://192.168.178.77:32408
2025-09-29 23:47:39 2 [Note] WSREP: Prepared IST receiver for 1245657-1245658, listening at: tcp://192.168.178.77:32408


--EVENT:  IST END - CONTEXT:  LOCAL
$ grep "IST received:" cl407/error.31407.log  
2025-09-25 18:04:00 2 [Note] WSREP: IST received: a572a681-97f2-11f0-9f63-c7c3a72b2527:1041405
2025-09-25 18:05:43 2 [Note] WSREP: IST received: a572a681-97f2-11f0-9f63-c7c3a72b2527:1055184
2025-09-25 18:08:14 2 [Note] WSREP: IST received: a572a681-97f2-11f0-9f63-c7c3a72b2527:1127403
2025-09-29 16:47:18 2 [Note] WSREP: IST received: a572a681-97f2-11f0-9f63-c7c3a72b2527:1245652
2025-09-29 23:47:45 2 [Note] WSREP: IST received: a572a681-97f2-11f0-9f63-c7c3a72b2527:1245658














