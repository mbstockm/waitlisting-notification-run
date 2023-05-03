import groovy.sql.Sql
import groovy.transform.Canonical

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.ResultSet
import java.time.LocalDateTime

/**
 * Checks waitlist terms that should be running and runs SFRBWLP notification process directly.
 * This is designed to ignore Banner sleep/wake and instead just call the SFRBWLP batch process at what ever freqeuncy we want on CRON.
 */
@GrabConfig(systemClassLoader = true)
@Grab(group = 'com.oracle',module = 'ojdbc8', version = 'current')

def waitlistTerms = []
Sql.withInstance(getDbProps()) { Sql sql ->
    waitlistTerms = getWaitlistTerms(sql)
    waitlistTerms.each { w ->
        runSfrbwlp(w.term,w.prnt)
    }
}

/**
 * Execute sub process to run SFRBWLP batch notification job only on the jobsub server.
 * Process checks OS name, and if windows the method ceases execution. Assuming other operating system would be LINUX jobsub server.
 * @author Michael Stockman
 */
void runSfrbwlp(def term, def prnt) {
    if (System.properties.'os.name'.toString().toUpperCase().contains('WINDOWS')) {
        println 'Skipped executing shell commands while running in windows development environment.'
        return
    }

    def t = LocalDateTime.now().format('yyyyMMddHHmmss')
    Path parameters = parameterFile(term,prnt,t)
    Path out = Paths.get(System.properties.'user.home','/gurjobs/PROD/waitlisting/sfrbwlp','sfrbwlp_' + term + '_' + t + '.out')
    Path lis = Paths.get(System.properties.'user.home','/gurjobs/PROD','sfrbwlp.lis')
    Path nlis = Paths.get(System.properties.'user.home','/gurjobs/PROD/waitlisting/sfrbwlp','sfrbwlp_' + term + '_' + t + '.lis')

    def env = System.getenv().collect {k,v -> "$k=$v"}
    env.add('TNS_ADMIN=/u03/banjobs/proxy_setup')
    env.add('TWO_TASK=PROD')

    def proc = ['/bin/bash','-c','sfrbwlp [SAISUSR]/@JSUB_PROD < ' + parameters.toAbsolutePath().toString() + ' > ' + out.toAbsolutePath().toString() + ' 2>&1']
            .execute(env,new File(System.properties.'user.home' + '/gurjobs/PROD')).waitFor()

    if (Files.exists(lis)) {
        Files.copy(lis,nlis)
        Files.delete(lis)
    }

}

/**
 * Creates parameter file for SFRBWLP job stdin and returns Path object
 * @param term
 * @return Path
 *
 * Connected.
 PARAMETER SEQUENCE NUMBER:

 PROCESS TERM: 202310

 PRINTER: wl_10

 RUN IN SLEEP/WAKE MODE (Y/[N]): N

 PRINT CONTROL REPORT (Y/[N]): Y

 NUMBER OF LINES PRINTED PER PAGE [55]: 55

 sfrbwlp completed successfully
 0 lines written to sfrbwlp.lis

 */
def parameterFile(def term, def prnt, def t) {
    Path path = Paths.get(System.properties.'user.home','/gurjobs/PROD/waitlisting/sfrbwlp','sfrbwlp_' + term + '_' + t + '.parm')
    path.withWriter { bw ->
        bw.write '\n'
        bw << term << '\n'
        bw << prnt << '\n'
        bw << 'N' << '\n'
        bw << 'Y' << '\n'
        bw << '55'
    }
    return path
}

/**
 * list of active waitlistTerm objects to be processed
 * @author Michael Stockman
 * @param sql
 * @return List of waitlistTerm objects
 */
def getWaitlistTerms(Sql sql) {
    def waitlistTerms = []
    sql.query(
            """select sobwltc_term_code term
                         ,gtvprnt_code prnt 
                     from sobwltc,gtvprnt
                    where sobwltc_auto_notify_ind = 'Y'
                      and sobwltc_online_notify_ind = 'Y'
                      and gtvprnt_code = 'wl_' || substr(sobwltc_term_code,5,2)
                      and exists (select 1
                                    from sfrrsts rsts
                                   where rsts.sfrrsts_term_code = sobwltc_term_code
                                     and rsts.sfrrsts_rsts_code = 'WL'
                                  having sysdate between min(rsts.sfrrsts_start_date) and max(sfrrsts_end_date))"""
    ) { ResultSet rs ->
        while (rs.next()) {
            WaitlistTerm waitlistTerm =
                    new WaitlistTerm(
                            rs.getString('TERM'),
                            rs.getString('PRNT'),
                    )
            waitlistTerms.add(waitlistTerm)
        }
    }
    return waitlistTerms
}

/**
 * Returns properties object with database credentials from file under user
 * @author Michael Stockman
 * @return Properties object
 */
def getDbProps() {
    Properties dbProps = new Properties()
    Paths.get(System.properties.'user.home', '.credentials', 'bannerProduction.properties').withInputStream {
        dbProps.load(it)
    }
    return dbProps
}

/**
 * Class pojo for waitlist term query information to drive SFRBWLP restart
 * @author Michael Stockman
 */
@Canonical
class WaitlistTerm {
    def term
    def prnt
}