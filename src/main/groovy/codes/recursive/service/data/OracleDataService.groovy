package codes.recursive.service.data

import codes.recursive.model.BarnEvent
import groovy.json.JsonSlurper
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.transform.CompileStatic
import io.micronaut.context.annotation.Property
import oracle.jdbc.pool.OracleConnectionPoolDataSource
import oracle.sql.TIMESTAMP
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.inject.Singleton
import java.sql.Clob
import java.sql.SQLException
import java.text.SimpleDateFormat

@Singleton
@CompileStatic
class OracleDataService {
    String oracleUrl
    String oracleUser
    String oraclePassword
    OracleConnectionPoolDataSource dataSource
    Sql defaultConnection

    static final Logger logger = LoggerFactory.getLogger(OracleDataService.class)

    OracleDataService(
            @Property(name="codes.recursive.oracle.db.user") String user,
            @Property(name="codes.recursive.oracle.db.password") String password,
            @Property(name="codes.recursive.oracle.db.url") String url
    ) {
        this.oracleUrl = url
        this.oracleUser = user
        this.oraclePassword = password
        this.dataSource = new OracleConnectionPoolDataSource()
        this.dataSource.user = this.oracleUser
        this.dataSource.password = this.oraclePassword
        this.dataSource.setURL(this.oracleUrl)
        this.defaultConnection = getDefaultConnection()
    }

    Sql getDefaultConnection() throws SQLException {
        Sql connection = Sql.newInstance( this.dataSource )
        return connection
    }

    def save(BarnEvent barnEvent) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        List<Object> params = [barnEvent.type, barnEvent.data, simpleDateFormat.format(barnEvent.capturedAt)] as List<Object>
        Sql connection = Sql.newInstance( this.dataSource )
        connection.execute("""
          insert into BARN.BARN_EVENT (TYPE, DATA, CAPTURED_AT) values (?, ?, to_timestamp(?, 'yyyy-mm-dd HH24:mi:ss'))
        """, params)
        connection.close()
    }

    int countEvents() {
        return defaultConnection.firstRow("select count(1) as NUM from BARN_EVENT")?.NUM as int ?: 0
    }

    int countEventsByEventType(String type) {
        return defaultConnection.firstRow("select count(1) as NUM from BARN_EVENT where TYPE = ?", [type] as List<Object>)?.NUM as int ?: 0
    }

    List listEventsByEventType(String type, int offset=0, int max=50) {
        List<Object> events = []
        JsonSlurper slurper = new JsonSlurper()
        defaultConnection.eachRow("select * from BARN_EVENT where TYPE = ?", [type] as List<Object>, offset, max) {
            events << [
                    id: it['ID'],
                    type: it['TYPE'],
                    capturedAt: (it['CAPTURED_AT'] as TIMESTAMP).stringValue(),
                    data:  slurper.parseText((it['DATA'] as Clob)?.asciiStream?.text),
            ]
        }
        return events
    }

    List listEvents(int offset=0, int max=50) {
        List events = []
        JsonSlurper slurper = new JsonSlurper()
        defaultConnection.eachRow("select * from BARN_EVENT", offset, max) {
            events << [
                    id: it['ID'],
                    type: it['TYPE'],
                    capturedAt: (it['CAPTURED_AT'] as TIMESTAMP).stringValue(),
                    data:  slurper.parseText((it['DATA'] as Clob)?.asciiStream?.text),
            ]
        }
        return events
    }

    void close() {
        logger.info "Closing SQL connection..."
        this.defaultConnection.close()
        logger.info "Closed SQL connection..."
    }
}
