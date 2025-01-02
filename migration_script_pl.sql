DECLARE
    v_error_message VARCHAR2(4000); -- Premenná na uchovanie chybovej správy / Variable to hold error message
    v_row_count     NUMBER := 0; -- Počet presunutých riadkov / Number of rows migrated
    v_table_size    NUMBER := 0; -- Veľkosť tabuľky / Size of the table
    v_batch_size    CONSTANT NUMBER := 100; -- Veľkosť dávky / Batch size
    v_offset        NUMBER := 0; -- Offset pre dávkovanie / Offset for batching
    v_retry_count   NUMBER := 0; -- Počet pokusov o migráciu / Number of migration attempts
    v_max_retries   CONSTANT NUMBER := 3; -- Maximálny počet pokusov / Maximum number of retries
    v_success       BOOLEAN := FALSE; -- Flag na sledovanie úspechu migrácie / Flag to track migration success
BEGIN
    -- Získanie veľkosti zdrojovej tabuľky / Get the size of the source table
    SELECT COUNT(*)
    INTO v_table_size
    FROM source_table;

    -- Kontrola, či existujú dáta na migráciu / Check if there are data to migrate
    IF v_table_size = 0 THEN
        DBMS_OUTPUT.PUT_LINE('Žiadne dáta na migráciu. Veľkosť tabuľky: ' || v_table_size); -- No data to migrate
        RETURN;
    END IF;

    -- Dávková migrácia s opakovaním / Batch migration with retries
    WHILE v_retry_count < v_max_retries AND NOT v_success LOOP
        BEGIN
            -- Resetovanie offsetu pre každú novú migráciu / Reset offset for each new migration
            v_offset := 0;
            v_row_count := 0;

            LOOP
                -- Presunúť dáta z source_table do destination_table po dávkach / Move data from source_table to destination_table in batches
                INSERT INTO destination_table (column1, column2, column3)
                SELECT column1, column2, column3
                FROM (SELECT column1, column2, column3
                      FROM source_table
                      WHERE 1=1 -- Podmienka na filtrovanie dát / Condition for filtering data
                      OFFSET v_offset ROWS FETCH NEXT v_batch_size ROWS ONLY);

                v_row_count := SQL%ROWCOUNT; -- Počet presunutých riadkov / Number of rows migrated

                -- Ak neexistujú ďalšie dáta, ukončiť cyklus / If no more data, exit the loop
                EXIT WHEN v_row_count = 0;

                COMMIT; -- Uložiť zmeny pre aktuálnu dávku / Commit changes for the current batch
                DBMS_OUTPUT.PUT_LINE('Migrácia dávky bola úspešná. Počet presunutých riadkov: ' || v_row_count); -- Batch migration successful

                -- Logovanie migrácie / Logging migration
                INSERT INTO migration_log (migration_date, rows_migrated, error_message)
                VALUES (SYSTIMESTAMP, v_row_count, NULL);

                v_offset := v_offset + v_batch_size; -- Posunúť offset pre ďalšiu dávku / Move offset for the next batch
            END LOOP;

            -- Voliteľne: Odstrániť presunuté dáta zo zdrojovej tabuľky / Optionally: Remove migrated data from the source table
            DELETE FROM source_table
            WHERE 1=1; -- Rovnaká podmienka ako pri vkladaní / Same condition as during insertion

            COMMIT; -- Uložiť zmeny po odstránení / Commit changes after deletion
            DBMS_OUTPUT.PUT_LINE('Migrácia dát bola úspešná. Celkový počet presunutých riadkov: ' || v_table_size); -- Migration successful

            -- Logovanie migrácie po úspešnej migrácii / Logging migration after successful migration
            INSERT INTO migration_log (migration_date, rows_migrated, error_message)
            VALUES (SYSTIMESTAMP, v_table_size, NULL);

            v_success := TRUE; -- Označiť migráciu ako úspešnú / Mark migration as successful

        EXCEPTION
            WHEN DUP_VAL_ON_INDEX THEN
                v_error_message := 'Chyba: Duplicita hodnoty v indexe.'; -- Error: Duplicate value in index
            WHEN OTHERS THEN
                v_error_message := 'Chyba: ' || SQLERRM; -- Error: Other error

                ROLLBACK; -- V prípade chyby vrátiť zmeny / Rollback changes in case of error

                -- Logovanie chyby / Logging error
                INSERT INTO migration_log (migration_date, rows_migrated, error_message)
                VALUES (SYSTIMESTAMP, 0, v_error_message);

                DBMS_OUTPUT.PUT_LINE(v_error_message); -- Vypísať chybovú správu / Print error message
                v_retry_count := v_retry_count + 1; -- Zvyšovať počet pokusov / Increment retry count
                DBMS_OUTPUT.PUT_LINE('Pokúšam sa o migráciu znova. Pokus č.: ' || (v_retry_count + 1)); -- Attempting to retry migration
        END;
    END LOOP;

    IF NOT v_success THEN
        DBMS_OUTPUT.PUT_LINE('Migrácia zlyhala po ' || v_max_retries || ' pokusoch.'); -- Migration failed after max retries
    END IF;

END;