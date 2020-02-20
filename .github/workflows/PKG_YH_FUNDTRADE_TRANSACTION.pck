CREATE OR REPLACE PACKAGE PKG_YH_FUNDTRADE_TRANSACTION IS
  TYPE T_CURSOR IS REF CURSOR;

  -- AUTHOR  : GERRY
  -- CREATED : 2018/4/12 16:02:43
  -- PURPOSE : 资金支付交易处理——交易程序调用
  
  --获取未交易数据——单笔
  PROCEDURE UPR_GETFUNDTRADENOT(OUT_DATA OUT T_CURSOR);
  
  --获取未交易数据——批量
  PROCEDURE UPR_GETFUNDTRADENOTBATCH(OUT_DATA OUT T_CURSOR);
  
  --交易结果回写——单笔
  PROCEDURE UPR_CALLBACKFUNDTRADE(IN_YWLSH IN VARCHAR2, IN_TRADESTATE IN NUMBER, IN_TRADEMSG IN VARCHAR2, IN_YHJSLSH IN VARCHAR2, OUT_MSG OUT VARCHAR2);
  
  --交易结果回写——批量
  PROCEDURE UPR_CALLBACKFUNDTRADEBATCH(IN_PLLSH IN VARCHAR2, IN_TRADESTATUS IN NUMBER, IN_DETAILXML IN CLOB, OUT_MSG OUT VARCHAR2);

END PKG_YH_FUNDTRADE_TRANSACTION;

 

 
/
CREATE OR REPLACE PACKAGE BODY PKG_YH_FUNDTRADE_TRANSACTION IS

	--获取未交易数据——单笔
  PROCEDURE UPR_GETFUNDTRADENOT(OUT_DATA OUT T_CURSOR)AS
    V_JYLSH         VARCHAR2(50 CHAR);
    V_WORKTYPECODE  VARCHAR2(50 CHAR);
    V_YWLSH         VARCHAR2(50 CHAR);
  BEGIN
    PKG_SYS_METHOD.UPR_SYS_GETSERIALNUMBER('PaySerial','',V_JYLSH);  
    
    UPDATE UTB_YH_FUNDTRADE_NOT A SET A.JYLSH = V_JYLSH WHERE A.ISBATCH = 0;
    
    FOR V_D IN (SELECT * FROM UTB_YH_FUNDTRADE_NOT A WHERE A.JYLSH = V_JYLSH) LOOP
      
      SELECT B.WORKTYPECODE INTO V_WORKTYPECODE FROM UTB_YH_FUNDTRADE B WHERE B.PLLSH = V_D.PLLSH;
      
      IF V_WORKTYPECODE IN ('TQ', 'YDZC', 'ZFTH_TQ', 'ZFTH_TQ_YDZC') THEN
        SELECT B.YWLSH INTO V_YWLSH FROM UTB_YH_FUNDTRADE_DETAIL B WHERE B.PLLSH = V_D.PLLSH AND ROWNUM = 1;
        
        UPDATE UTB_GA_EXECMONEYDRAW B SET B.TRADESTATE = 1, B.TRADEMSG = '' WHERE B.WORKSERIALNO = V_YWLSH;
      END IF;
      
      UPDATE UTB_YH_FUNDTRADE B SET B.TRADESTATE = 1, B.TRADEDATE = SYSDATE WHERE B.PLLSH = V_D.PLLSH;
      
      UPDATE UTB_YH_FUNDTRADE_DETAIL B SET B.TRADESTATE = 1 WHERE B.PLLSH = V_D.PLLSH;
    END LOOP;
    
    OPEN OUT_DATA FOR
    SELECT A.MXLSH, A.PLLSH, A.JYLSH, A.ISBATCH, A.BUSITYPE, A.DEBANKCODE, A.DEACCTNO, A.DEACCTNAME, A.CRBANKNAME, A.CRACCTNO, A.CRACCTNAME, A.CRBANKCLASS,
    CASE WHEN A.CRBANKCLASS = 0 THEN 1 ELSE 3 END AS SETTLETYPE, A.CRCHGNO, TO_CHAR(A.FSE) AS FSE, A.SUMMARK, A.REMARK FROM UTB_YH_FUNDTRADE_NOT A WHERE A.JYLSH = V_JYLSH;
    
    DELETE UTB_YH_FUNDTRADE_NOT A WHERE A.JYLSH = V_JYLSH;
    
    EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      OPEN OUT_DATA FOR SELECT NULL FROM DUAL;
      RETURN;
  END UPR_GETFUNDTRADENOT;
  
  --获取未交易数据——批量
  PROCEDURE UPR_GETFUNDTRADENOTBATCH(OUT_DATA OUT T_CURSOR)AS
    V_JYLSH     VARCHAR2(50 CHAR);
  BEGIN
    PKG_SYS_METHOD.UPR_SYS_GETSERIALNUMBER('PaySerial','',V_JYLSH);  
    
    UPDATE UTB_YH_FUNDTRADE_NOT A SET A.JYLSH = V_JYLSH WHERE A.ISBATCH = 1;
    
    FOR V_D IN (SELECT * FROM UTB_YH_FUNDTRADE_NOT A WHERE A.JYLSH = V_JYLSH) LOOP
      UPDATE UTB_YH_FUNDTRADE B SET B.TRADESTATE = 1, B.TRADEDATE = SYSDATE WHERE B.PLLSH = V_D.PLLSH;
      
      UPDATE UTB_YH_FUNDTRADE_DETAIL B SET B.TRADESTATE = 1 WHERE B.PLLSH = V_D.PLLSH;
    END LOOP;
    
    OPEN OUT_DATA FOR
    SELECT B.PLLSH, B.DEBANKCODE, B.DEACCTNO, B.DEACCTNAME, B.BUSITYPE, TO_CHAR(B.FSE) AS FSE_SUM, B.FSBS, B.ZHAIYAO, B.DEBANKID,
    C.ORDERNO, TO_CHAR(C.FSE) AS FSE, C.CRACCTNO, C.CRACCTNAME, C.CRCHGNO, C.SUMMARY, C.REMARK, C.MXLSH, C.ZJHM, C.CRBANKID
    FROM UTB_YH_FUNDTRADE_NOT A LEFT JOIN UTB_YH_FUNDTRADE B ON A.PLLSH = B.PLLSH
    LEFT JOIN UTB_YH_FUNDTRADE_DETAIL C ON A.PLLSH = C.PLLSH AND A.MXLSH = C.MXLSH WHERE A.JYLSH = V_JYLSH;
    
    DELETE UTB_YH_FUNDTRADE_NOT A WHERE A.JYLSH = V_JYLSH;
    
    EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      OPEN OUT_DATA FOR SELECT NULL FROM DUAL;
      RETURN;
  END UPR_GETFUNDTRADENOTBATCH;
  
  --交易结果回写——单笔
  PROCEDURE UPR_CALLBACKFUNDTRADE(IN_YWLSH IN VARCHAR2, IN_TRADESTATE IN NUMBER, IN_TRADEMSG IN VARCHAR2, IN_YHJSLSH IN VARCHAR2, OUT_MSG OUT VARCHAR2)AS
    V_EXISTS           NUMBER(10);
    V_TRADESTATE       NUMBER(10);
    V_YWLSH            VARCHAR2(50 CHAR);
    V_WORKTYPECODE     VARCHAR2(50 CHAR);
    V_FSE              NUMBER(18,2);
    V_ZHAIYAO          VARCHAR2(500 CHAR);
    V_TRADEDATE        DATE;
    V_YWAREAID         NUMBER(10);
    V_YWDZTZID         NUMBER(20);
    V_APPLYIDS         VARCHAR2(4000 CHAR);
    V_APPLYIDS_NEW     VARCHAR2(4000 CHAR);
    V_AREACODE         VARCHAR2(10);
    V_DEBANKACCID      NUMBER(10);
    V_EXTENDS          CLOB;
    V_INDVID           NUMBER(10);
    V_ISABNORMALTRADE  NUMBER(10);
    V_MXLSH            VARCHAR2(50 CHAR);
    V_TRADESTATE_PL    NUMBER(10);
    V_PLLSH            VARCHAR2(50 CHAR);
    V_JYBM             VARCHAR2(50 CHAR);
    V_SENDOPT          VARCHAR2(50 CHAR);
    V_SMSCONTEXT       VARCHAR2(500 CHAR);
    V_PHONE            VARCHAR2(50 CHAR);
    V_SMS_CODE         VARCHAR2(50 CHAR);
    V_DEVELOPERID      NUMBER(10);
    V_XINGMING         VARCHAR2(500 CHAR);
    V_ZJHM             VARCHAR2(500 CHAR);
    V_DHXKLSH          VARCHAR2(50 CHAR);
    V_WORKTYPECODE_NEW VARCHAR2(50 CHAR);
    V_CRBANKID         NUMBER(10);
    V_CRACCTNO         VARCHAR2(50 CHAR);
    V_CRACCTNAME       VARCHAR2(255 CHAR);
    V_CRCHGNO          VARCHAR2(50 CHAR);
    V_CRZJHM           VARCHAR2(50 CHAR);
    V_ZJLSH            VARCHAR2(50 CHAR);
  BEGIN
    IF IN_TRADESTATE = 1 THEN
      V_TRADESTATE := 2;
    ELSE
      V_TRADESTATE := 3;
    END IF;  
  
    SELECT COUNT(*) INTO V_EXISTS FROM UTB_YH_FUNDTRADE_DETAIL A WHERE A.PLLSH = IN_YWLSH;
    
    IF V_EXISTS = 0 THEN
      OUT_MSG := '业务流水号不存在';
      RETURN;
    END IF;
    
    SELECT B.WORKTYPECODE, B.TRADEDATE, A.YWLSH, B.YWAREAID, B.DEBANKACCID, A.EXTENDS, A.FSE, A.REMARK, NVL(B.ISABNORMALTRADE, 0), A.MXLSH, B.TRADESTATE, B.SENDOPT
    INTO V_WORKTYPECODE, V_TRADEDATE, V_YWLSH, V_YWAREAID, V_DEBANKACCID, V_EXTENDS, V_FSE, V_ZHAIYAO, V_ISABNORMALTRADE, V_MXLSH, V_TRADESTATE_PL, V_SENDOPT
    FROM UTB_YH_FUNDTRADE_DETAIL A LEFT JOIN UTB_YH_FUNDTRADE B ON A.PLLSH = B.PLLSH WHERE A.PLLSH = IN_YWLSH;
    
    IF V_TRADESTATE_PL = 2 OR V_TRADESTATE_PL = 3 THEN
      OUT_MSG := '该笔交易结果已回写';
      RETURN;
    END IF;
    
    IF IN_TRADESTATE = 1 THEN
      UPDATE UTB_YH_FUNDTRADE A SET A.TRADESTATE = 2 WHERE A.PLLSH = IN_YWLSH;  
    ELSE
      UPDATE UTB_YH_FUNDTRADE A SET A.TRADESTATE = 3 WHERE A.PLLSH = IN_YWLSH;
    END IF;
    
    PKG_YH_FUNDTRADELOG.UPR_SAVEFUNDTRADELOG(IN_YWAREAID      => 0, 
                                             IN_WORKTYPECODE  => V_WORKTYPECODE, 
                                             IN_YWLSH         => V_YWLSH, 
                                             IN_MXLSH         => V_MXLSH, 
                                             IN_FSE           => V_FSE, 
                                             IN_DEBANKACCID   => V_DEBANKACCID, 
                                             IN_ZHAIYAO       => IN_TRADEMSG, 
                                             IN_LCMC          => '支付申请-交易',  
                                             IN_OPTNAME       => 'system',  
                                             IN_OPTCODE       => '',  
                                             OUT_MSG          => OUT_MSG);
                                                   
    IF OUT_MSG IS NOT NULL THEN
      ROLLBACK;
      RETURN;
    END IF;
      
    UPDATE UTB_YH_FUNDTRADE_DETAIL A SET A.TRADESTATE = V_TRADESTATE, A.TRADEMSG = IN_TRADEMSG, A.YHJSLSH = IN_YHJSLSH 
    WHERE A.PLLSH = IN_YWLSH;
    
    -- 提取、异地转出 回写执行表交易状态 1成功 2失败
    IF V_WORKTYPECODE IN ('TQ', 'ZFTH_TQ', 'YDZC', 'ZFTH_TQ_YDZC') THEN
      UPDATE UTB_GA_EXECMONEYDRAW A SET A.TRADESTATE = V_TRADESTATE, A.TRADEMSG = IN_TRADEMSG WHERE A.WORKSERIALNO = V_YWLSH;  
      
      IF IN_TRADESTATE = 1 THEN
        SELECT A.CRBANKID, A.CRACCTNO, A.CRACCTNAME, A.CRCHGNO, A.ZJHM INTO V_CRBANKID, V_CRACCTNO, V_CRACCTNAME, V_CRCHGNO, V_CRZJHM
        FROM UTB_YH_FUNDTRADE_DETAIL A WHERE A.PLLSH = IN_YWLSH;
        
        UPDATE UTB_GA_EXECMONEYDRAW A SET A.BANKID = V_CRBANKID, A.OPENNAME = V_CRACCTNAME, A.BANKACC = V_CRACCTNO, A.BANKLINKNO = V_CRCHGNO, 
        A.BANKIDCARD = V_CRZJHM, A.BANKACCID = V_DEBANKACCID WHERE A.WORKSERIALNO = V_YWLSH;
      END IF;
    END IF;
    
    IF V_WORKTYPECODE IN ('TQ', 'BZJTQ', 'GZTQ', 'QYHD', 'SDQY', 'TXDK', 'YDZC', 'BZJGZTQ', 'BZJKH', 'DHXKTQ') THEN
      IF V_WORKTYPECODE IN ('TQ', 'YDZC') THEN
        SELECT B.DWMC || ',' || A.ABSTRACT, B.AREAID INTO V_ZHAIYAO, V_YWAREAID
        FROM UTB_GA_EXECMONEYDRAW A LEFT JOIN UTB_GA_ETPSINFO B ON A.ETPSID = B.ETPSID WHERE A.WORKSERIALNO = V_YWLSH;        
        
        --写入资金表数据
        PKG_GA_MONEYINDVDRAW.UPR_SAVEMONEYDRAWTOFUND(IN_WORKSERIALNO    => V_YWLSH,
                                                     IN_CALINTERDATE    => V_TRADEDATE,
                                                     OUT_FUNDSERIALNO   => V_ZJLSH,
                                                     OUT_MSG            => OUT_MSG);
        IF OUT_MSG IS NOT NULL THEN
          ROLLBACK;
          RETURN;
        END IF;
        
        UPDATE UTB_YH_FUNDTRADE_DETAIL A SET A.ZJLSH = V_ZJLSH WHERE A.MXLSH = V_MXLSH AND A.PLLSH = IN_YWLSH;
      END IF;
               
      IF V_WORKTYPECODE = 'BZJTQ' THEN
        SELECT COUNT(*) INTO V_EXISTS
        FROM UTB_MARGIN_DETAIL A LEFT JOIN UTB_MARGIN_MAIN B ON A.MARGINID = B.MARGINID
        LEFT JOIN UTB_DK_CUSTOMER C ON A.APPLYID = C.APPLYID AND C.CDGX = '01' 
        WHERE B.SERIALCODE = V_YWLSH;
          
        IF V_EXISTS > 4 THEN
          SELECT LISTAGG(C.GTJKRXM, ',') WITHIN GROUP(ORDER BY C.GTJKRXM) || '等' INTO V_ZHAIYAO 
          FROM UTB_MARGIN_DETAIL A LEFT JOIN UTB_MARGIN_MAIN B ON A.MARGINID = B.MARGINID
          LEFT JOIN UTB_DK_CUSTOMER C ON A.APPLYID = C.APPLYID AND C.CDGX = '01' 
          WHERE B.SERIALCODE = V_YWLSH AND ROWNUM < 5;  
        ELSE
          SELECT LISTAGG(C.GTJKRXM, ',') WITHIN GROUP(ORDER BY C.GTJKRXM) INTO V_ZHAIYAO 
          FROM UTB_MARGIN_DETAIL A LEFT JOIN UTB_MARGIN_MAIN B ON A.MARGINID = B.MARGINID
          LEFT JOIN UTB_DK_CUSTOMER C ON A.APPLYID = C.APPLYID AND C.CDGX = '01' 
          WHERE B.SERIALCODE = V_YWLSH;  
        END IF;  
        
        SELECT B.DEVELOPERNAME || '的'|| V_ZHAIYAO || '退取保证金', B.AREAID INTO V_ZHAIYAO, V_YWAREAID
        FROM UTB_MARGIN_MAIN A LEFT JOIN UTB_DK_DEVELOPER B ON A.DEVELOPERID = B.DEVELOPERID
        WHERE A.SERIALCODE = V_YWLSH;
           
        UPDATE UTB_MARGIN_MAIN A SET A.ACCSTATE = 1, A.CALINTDATE = SYSDATE WHERE A.SERIALCODE = V_YWLSH;
          
        UPDATE UTB_MARGIN_DETAIL A SET A.CALINTDATE = SYSDATE WHERE A.MARGINID = (SELECT B.MARGINID FROM UTB_MARGIN_MAIN B WHERE B.SERIALCODE = V_YWLSH);
      END IF;    
      
      IF V_WORKTYPECODE = 'BZJGZTQ' THEN
        SELECT A.REMARK, A.AREAID INTO V_ZHAIYAO, V_YWAREAID
        FROM UTB_MARGIN_PAYFUND A WHERE A.SERIALCODE = V_YWLSH;
           
        UPDATE UTB_MARGIN_PAYFUND A SET A.ACCSTATE = 1 WHERE A.SERIALCODE = V_YWLSH;
          
      END IF;      
         
      IF V_WORKTYPECODE = 'GZTQ' THEN
        SELECT B.DWMC || '挂账提取', B.AREAID INTO V_ZHAIYAO, V_YWAREAID
        FROM UTB_GA_EXECETPSSPAREDRAW A LEFT JOIN UTB_GA_ETPSINFO B ON A.ETPSID = B.ETPSID WHERE A.WORKSERIALNO = V_YWLSH;  
            
        PKG_GA_ETPSSPAREDRAW.UPR_SAVESPAREDRAWTOFUND(IN_WORKSERIALNO  => V_YWLSH,
                                                     IN_CALINTERDATE  => V_TRADEDATE,
                                                     OUT_FUNDSERIALNO => V_ZJLSH,
                                                     OUT_MSG          => OUT_MSG);
             
        IF OUT_MSG IS NOT NULL THEN
          ROLLBACK;
          RETURN;
        END IF;      
        
        UPDATE UTB_YH_FUNDTRADE_DETAIL A SET A.ZJLSH = V_ZJLSH WHERE A.MXLSH = V_MXLSH AND A.PLLSH = IN_YWLSH;
      END IF;
        
      IF V_WORKTYPECODE = 'QYHD' THEN
        SELECT C.DWMC || ',' || B.XINGMING || '签约还贷支取', A.CURINDVID, B.AREAID INTO V_ZHAIYAO, V_INDVID, V_YWAREAID
        FROM UTB_GA_DFLPLANEXPLODE A LEFT JOIN UTB_GA_INDVINFO B ON A.CURINDVID = B.INDVID
        LEFT JOIN UTB_GA_ETPSINFO C ON B.ETPSID = C.ETPSID LEFT JOIN UTB_GA_DFLPLANMAIN D ON A.PLANID = D.PLANID
        WHERE TO_CHAR(A.EXPID) = V_YWLSH AND D.PLANSERIAL = TO_CHAR(V_EXTENDS); 
          
        --写入资金表数据
        PKG_GA_DFLPLANAPPLY_CHECK.UPR_GA_SAVEDFLTOFUND(IN_PLANSERIAL    => TO_CHAR(V_EXTENDS), 
                                                       IN_INDVID        => V_INDVID,
                                                       IN_CALINTERDATE  => V_TRADEDATE,
                                                       OUT_FUNDSERIALNO => V_ZJLSH,
                                                       OUT_MSG          => OUT_MSG);
           
        IF OUT_MSG IS NOT NULL THEN
          ROLLBACK;
          RETURN;
        END IF;
        
        UPDATE UTB_YH_FUNDTRADE_DETAIL A SET A.ZJLSH = V_ZJLSH WHERE A.MXLSH = V_MXLSH AND A.PLLSH = IN_YWLSH;
      END IF;
      
      IF V_WORKTYPECODE = 'SDQY' THEN
        SELECT C.DWMC || ',' || B.XINGMING || '商贷签约支取', A.CURINDVID, B.AREAID INTO V_ZHAIYAO, V_INDVID, V_YWAREAID
        FROM UTB_GA_DFBPLANEXPLODE A LEFT JOIN UTB_GA_INDVINFO B ON A.CURINDVID = B.INDVID
        LEFT JOIN UTB_GA_ETPSINFO C ON B.ETPSID = C.ETPSID LEFT JOIN UTB_GA_DFBPLANMAIN D ON A.PLANID = D.PLANID
        WHERE TO_CHAR(A.EXPID) = V_YWLSH AND D.PLANSERIAL = TO_CHAR(V_EXTENDS); 
          
        --写入资金表数据
        PKG_GA_DFBPLANAPPLY_CHECK.UPR_GA_SAVEDFBTOFUND(IN_PLANSERIAL    => TO_CHAR(V_EXTENDS), 
                                                       IN_INDVID        => V_INDVID,
                                                       IN_CALINTERDATE  => V_TRADEDATE,
                                                       OUT_FUNDSERIALNO => V_ZJLSH,
                                                       OUT_MSG          => OUT_MSG);
           
        IF OUT_MSG IS NOT NULL THEN
          ROLLBACK;
          RETURN;
        END IF;
        
        UPDATE UTB_YH_FUNDTRADE_DETAIL A SET A.ZJLSH = V_ZJLSH WHERE A.MXLSH = V_MXLSH AND A.PLLSH = IN_YWLSH;
      END IF;
    
      IF V_WORKTYPECODE = 'TXDK' THEN
        SELECT A.AREAID INTO V_YWAREAID FROM UTB_DK_INTERESTMAIN_TX A WHERE A.WORKSERIALNO = V_YWLSH;
      
        UPDATE UTB_DK_INTERESTMAIN_TX A SET A.PAYSTATE = 1 WHERE A.WORKSERIALNO = V_YWLSH;
          
      END IF;      
    
      IF V_WORKTYPECODE = 'BZJKH' THEN
        PKG_DK_MARGIN_DEDUCT.UPR_MARGIN_EXECDEDUCT(IN_SERIALCODE => V_YWLSH, 
                                                   IN_JZRQ       => V_TRADEDATE, 
                                                   OUT_MSG       => OUT_MSG);
        IF OUT_MSG IS NOT NULL THEN
          ROLLBACK;
          RETURN;
        END IF;
      END IF;
      
      IF V_WORKTYPECODE = 'DHXKTQ' THEN
        PKG_CN_VERIFICATION_TQ.UPR_TRADECALLBACK(IN_YWLSH      => V_YWLSH,
                                                 IN_TRADESTATE => V_TRADESTATE,
                                                 IN_TRADEMSG   => IN_TRADEMSG,
                                                 IN_TRADEDATE  => V_TRADEDATE,
                                                 OUT_MSG       => OUT_MSG);
        IF OUT_MSG IS NOT NULL THEN
          ROLLBACK;
          RETURN;
        END IF;
      END IF;
    
      --写入业务动账通知表数据  
      PKG_CN_MATCH.UPR_CN_INTERFACE_YWDZTZ(IN_FUNDTYPE     => 0,
                                           IN_WORKTYPECODE => V_WORKTYPECODE, 
                                           IN_YWLSH        => IN_YWLSH, 
                                           IN_JZRQ         => V_TRADEDATE,
                                           IN_FSE          => V_FSE,
                                           IN_ZHAIYAO      => V_ZHAIYAO, 
                                           IN_BANKACCID    => V_DEBANKACCID, 
                                           IN_OPTNAME      => '系统', 
                                           IN_AREAID       => V_YWAREAID,
                                           OUT_YWDZTZID    => V_YWDZTZID,
                                           OUT_MSG         => OUT_MSG);
      IF OUT_MSG IS NOT NULL THEN
        ROLLBACK;
        RETURN;
      END IF;
        
      UPDATE UTB_YH_FUNDTRADE_DETAIL A SET A.YWDZTZID = V_YWDZTZID WHERE A.PLLSH = IN_YWLSH;
      
    ELSIF (V_WORKTYPECODE = 'FK' OR V_WORKTYPECODE = 'ZFTH_TQ_FK') AND IN_TRADESTATE = 1 THEN
      
      V_AREACODE := PKG_SYS_FUNCTION.Ufn_Sys_Getconfig('AreaCode');
    
      IF V_WORKTYPECODE = 'FK' THEN
        UPDATE UTB_DK_GRANTAPPLY A SET A.PAYSTATE = 1, A.PAYDATE = SYSDATE, A.TRADESTATE = 2 WHERE A.PAYSERIALNO = IN_YWLSH;
        
        SELECT LISTAGG(A.APPLYID, ',')  WITHIN GROUP(ORDER BY A.APPLYID) INTO V_APPLYIDS  FROM UTB_DK_GRANTAPPLY A WHERE A.PAYSERIALNO = IN_YWLSH;
        
        IF(V_AREACODE = '0282') THEN
          --资阳需求：财务放款确认后自动银行放款确认 2017年10月9日23:44:19
          PKG_DK_APPLYGRANTOPEN_CHECK.UPR_DK_CHECKPASSCWAUTOCONFIRM(IN_APPLYIDS => V_APPLYIDS,
                                                                    IN_CHECKOPT    => V_SENDOPT,
                                                                    IN_CHECKUSERID => 0,
                                                                    OUT_MSG        => OUT_MSG);
        ELSE
          PKG_DK_APPLYGRANTOPEN_CHECK.UPR_DK_CHECKPASSCW(IN_APPLYIDS    => V_APPLYIDS,
                                                         IN_CHECKOPT    => V_SENDOPT,
                                                         IN_CHECKUSERID => 0,
                                                         OUT_MSG        => OUT_MSG);
              
        END IF;
            
        IF OUT_MSG IS NOT NULL THEN
          ROLLBACK;
          RETURN;
        END IF;  
        
      ELSE
        SELECT A.PLLSH INTO V_PLLSH FROM UTB_YH_FUNDTRADE_DETAIL A WHERE A.MXLSH = (SELECT B.YWLSH FROM UTB_YH_FUNDTRADE_DETAIL B WHERE B.PLLSH = IN_YWLSH);   
      
        SELECT TO_CHAR(A.EXTENDS) INTO V_APPLYIDS FROM UTB_YH_FUNDTRADE_DETAIL A WHERE A.PLLSH = IN_YWLSH;
        
        SELECT LISTAGG(A.APPLYID, ',')  WITHIN GROUP(ORDER BY A.APPLYID) INTO V_APPLYIDS_NEW 
        FROM UTB_DK_GRANTAPPLY A WHERE A.PAYSERIALNO = V_PLLSH AND A.PAYSTATE = 0 AND A.APPLYID IN (SELECT TO_NUMBER(COLUMN_VALUE) AS APPLYID FROM TABLE(PKG_SYS_FUNCTION.UFN_SYS_SPLITSTR(V_APPLYIDS,',')));
        
        IF V_APPLYIDS_NEW IS NOT NULL THEN
          UPDATE UTB_DK_GRANTAPPLY A SET A.PAYSTATE = 1, A.PAYDATE = SYSDATE, A.TRADESTATE = 2 WHERE A.PAYSERIALNO = V_PLLSH 
          AND A.APPLYID IN (SELECT TO_NUMBER(COLUMN_VALUE) AS APPLYID FROM TABLE(PKG_SYS_FUNCTION.UFN_SYS_SPLITSTR(V_APPLYIDS_NEW,',')));
        
          IF(V_AREACODE = '0282') THEN
            --资阳需求：财务放款确认后自动银行放款确认 2017年10月9日23:44:19
            PKG_DK_APPLYGRANTOPEN_CHECK.UPR_DK_CHECKPASSCWAUTOCONFIRM(IN_APPLYIDS => V_APPLYIDS_NEW,
                                                                      IN_CHECKOPT    => V_SENDOPT,
                                                                      IN_CHECKUSERID => 0,
                                                                      OUT_MSG        => OUT_MSG);
          ELSE
            PKG_DK_APPLYGRANTOPEN_CHECK.UPR_DK_CHECKPASSCW(IN_APPLYIDS    => V_APPLYIDS_NEW,
                                                           IN_CHECKOPT    => V_SENDOPT,
                                                           IN_CHECKUSERID => 0,
                                                           OUT_MSG        => OUT_MSG);
                
          END IF;
              
          IF OUT_MSG IS NOT NULL THEN
            ROLLBACK;
            RETURN;
          END IF;  
        END IF;
        
        UPDATE UTB_DK_GRANTAPPLY A SET A.TRADESTATE = 2 WHERE A.PAYSERIALNO = V_PLLSH 
        AND A.APPLYID IN (SELECT TO_NUMBER(COLUMN_VALUE) AS APPLYID FROM TABLE(PKG_SYS_FUNCTION.UFN_SYS_SPLITSTR(V_APPLYIDS,',')));
        
      END IF;  
    
      --写入业务动账通知表数据  
      PKG_CN_MATCH.UPR_CN_INTERFACE_YWDZTZ(IN_FUNDTYPE     => 0,
                                           IN_WORKTYPECODE => V_WORKTYPECODE, 
                                           IN_YWLSH        => IN_YWLSH, 
                                           IN_JZRQ         => V_TRADEDATE,
                                           IN_FSE          => V_FSE,
                                           IN_ZHAIYAO      => V_ZHAIYAO, 
                                           IN_BANKACCID    => V_DEBANKACCID, 
                                           IN_OPTNAME      => '系统', 
                                           IN_AREAID       => V_YWAREAID,
                                           OUT_YWDZTZID    => V_YWDZTZID,
                                           OUT_MSG         => OUT_MSG);
      IF OUT_MSG IS NOT NULL THEN
        ROLLBACK;
        RETURN;
      END IF;
        
      UPDATE UTB_YH_FUNDTRADE_DETAIL A SET A.YWDZTZID = V_YWDZTZID WHERE A.PLLSH = IN_YWLSH;
      
      V_JYBM := PKG_YH_PUBLIC.UFN_GETFKJYBM(V_YWAREAID, TO_CHAR(V_TRADEDATE, 'YYYYMM'));
      
      UPDATE UTB_YH_FUNDTRADE A SET A.JYBM = V_JYBM WHERE A.PLLSH = IN_YWLSH;
      
    ELSIF V_WORKTYPECODE = 'FK' AND IN_TRADESTATE <> 1 THEN
      SELECT LISTAGG(A.APPLYID, ',')  WITHIN GROUP(ORDER BY A.APPLYID) INTO V_APPLYIDS 
      FROM UTB_DK_GRANTAPPLY A WHERE A.PAYSERIALNO = IN_YWLSH;
      
      UPDATE UTB_DK_GRANTAPPLY A SET A.TRADESTATE = 1 WHERE A.PAYSERIALNO = IN_YWLSH;
    ELSIF V_WORKTYPECODE = 'ZFTH_TQ_FK' AND IN_TRADESTATE <> 1 THEN
      
      V_APPLYIDS := TO_CHAR(V_EXTENDS);
      
    ELSIF (SUBSTR(V_WORKTYPECODE, 0, 4) = 'ZJHB'
      OR V_WORKTYPECODE IN ('ZFTH_TQ','ZFTH_TQ_GZTQ', 'ZFTH_TQ_YDZC', 'ZFTH_TQ_SYHD', 'ZFTH_TQ_QYHD', 'ZFTH_TQ_BZJTQ', 'ZFTH_TQ_ZJHB', 'ZFTH_TQ_BZJGZTQ', 'ZFTH_TQ_BZJKH', 'ZFTH_TQ_DHXKTQ')) AND IN_TRADESTATE = 1 THEN
      --写入业务动账通知表数据  
      PKG_CN_MATCH.UPR_CN_INTERFACE_YWDZTZ(IN_FUNDTYPE     => 0,
                                           IN_WORKTYPECODE => V_WORKTYPECODE, 
                                           IN_YWLSH        => IN_YWLSH, 
                                           IN_JZRQ         => V_TRADEDATE,
                                           IN_FSE          => V_FSE,
                                           IN_ZHAIYAO      => V_ZHAIYAO, 
                                           IN_BANKACCID    => V_DEBANKACCID, 
                                           IN_OPTNAME      => '系统', 
                                           IN_AREAID       => V_YWAREAID,
                                           OUT_YWDZTZID    => V_YWDZTZID,
                                           OUT_MSG         => OUT_MSG);
      IF OUT_MSG IS NOT NULL THEN
        ROLLBACK;
        RETURN;
      END IF;
        
      UPDATE UTB_YH_FUNDTRADE_DETAIL A SET A.YWDZTZID = V_YWDZTZID WHERE A.PLLSH = IN_YWLSH;
      
      IF V_WORKTYPECODE = 'ZFTH_TQ_FK' THEN
        V_JYBM := PKG_YH_PUBLIC.UFN_GETFKJYBM(V_YWAREAID, TO_CHAR(V_TRADEDATE, 'YYYYMM'));
      
        UPDATE UTB_YH_FUNDTRADE A SET A.JYBM = V_JYBM WHERE A.PLLSH = IN_YWLSH;
      END IF;
      
      IF V_WORKTYPECODE = 'ZJHBDHXKZQ' THEN
        SELECT TO_CHAR(A.EXTENDS) INTO V_DHXKLSH FROM UTB_YH_FUNDTRADE_DETAIL A WHERE A.PLLSH = IN_YWLSH;
      
        PKG_YH_FUNDTRADE_SEND.UPR_SAVEDHXKTOEXEC(V_DHXKLSH, IN_YWLSH, OUT_MSG);
        
        IF OUT_MSG IS NOT NULL THEN
          ROLLBACK;
          RETURN;
        END IF;
      END IF;
      
      IF V_WORKTYPECODE = 'ZFTH_TQ_ZJHB' THEN
        SELECT C.WORKTYPECODE INTO V_WORKTYPECODE_NEW FROM UTB_YH_FUNDTRADE_DETAIL A LEFT JOIN UTB_YH_FUNDTRADE C ON A.PLLSH = C.PLLSH 
        WHERE A.MXLSH = (SELECT B.YWLSH FROM UTB_YH_FUNDTRADE_DETAIL B WHERE B.PLLSH = IN_YWLSH);   
        
        IF V_WORKTYPECODE_NEW = 'ZJHBDHXKZQ' THEN
          SELECT TO_CHAR(A.EXTENDS) INTO V_DHXKLSH FROM UTB_YH_FUNDTRADE_DETAIL A WHERE A.PLLSH = IN_YWLSH;
      
          PKG_YH_FUNDTRADE_SEND.UPR_SAVEDHXKTOEXEC(V_DHXKLSH, IN_YWLSH, OUT_MSG);
          
          IF OUT_MSG IS NOT NULL THEN
            ROLLBACK;
            RETURN;
          END IF;
        END IF;
      END IF;
    END IF;
    
    IF IN_TRADESTATE <> 1 THEN 
      IF V_ISABNORMALTRADE = 0 THEN
        PKG_YH_FUNDTRADE_ABNORMAL.UPR_SAVEFUNDTRADE_ABNORMAL(IN_PLLSH      => IN_YWLSH, 
                                                             IN_MXLSHS     => V_MXLSH, 
                                                             IN_TRADEMSG   => IN_TRADEMSG, 
                                                             IN_INFOSOURCE => 0, 
                                                             IN_EXTENDS    => V_APPLYIDS, 
                                                             IN_OPTNAME    => '系统', 
                                                             IN_OPTCODE    => '', 
                                                             OUT_MSG       => OUT_MSG);
        IF OUT_MSG IS NOT NULL THEN
          ROLLBACK;
          RETURN;
        END IF;
        
        UPDATE UTB_YH_FUNDTRADE A SET A.ISABNORMALTRADE = 1 WHERE A.PLLSH = IN_YWLSH;
      END IF;
      
      IF V_YWDZTZID <> 0 THEN
        UPDATE UTB_CN_YWDZTZ A SET A.PPZT = 1, A.PPRQ = SYSDATE, A.PPOPTNAME = '系统' WHERE A.YWDZTZ_ID = V_YWDZTZID;  
      END IF;
    END IF;
    
    IF IN_TRADESTATE <> 1 AND (V_WORKTYPECODE = 'TQ' OR V_WORKTYPECODE = 'ZFTH_TQ') THEN
      --判断是否发送短信
      SELECT COUNT(*) INTO V_EXISTS FROM UTB_SMS_CONFIG A WHERE A.WORKCODE = 'GA_ZGTQ_SB' AND A.ISUSED = 1;
      
      IF V_EXISTS > 0 THEN
        --获取发送短信内容
        V_SMSCONTEXT := PKG_SMS_SEND.UFN_CREATECONTEXT(IN_WORKID   => V_YWLSH, 
                                                       IN_WORKCODE => 'GA_ZGTQ_SB',
                                                       IN_YWMXID   => '');
        IF V_SMSCONTEXT IS NULL THEN
          ROLLBACK;
          OUT_MSG := '获取短信内容失败！';
          RETURN;
        END IF;
        
        SELECT A.INDVID, A.XINGMING, A.ZJHM INTO V_INDVID, V_XINGMING, V_ZJHM FROM UTB_GA_EXECMONEYDRAW A WHERE A.WORKSERIALNO = V_YWLSH;
        
        --获取手机号码
        V_PHONE := PKG_SMS_SEND.UFN_GETSMSSJHM(IN_CUSTTYPE => 0, 
                                               IN_CUSTID   => V_INDVID, 
                                               IN_YHMC     => V_XINGMING, 
                                               IN_YHBM     => V_ZJHM);
      
        IF V_PHONE IS NOT NULL THEN
          --保存短信内容到短信表
          PKG_SMS_SEND.UPR_SENDSMS(IN_WORKCODE => 'GA_ZGTQ_SB',
                                   IN_PHONE    => V_PHONE,
                                   IN_CONTEXT  => V_SMSCONTEXT);
        END IF;

      END IF;
    END IF;
    
    IF IN_TRADESTATE = 1 AND V_WORKTYPECODE = 'ZFTH_TQ' THEN
      --判断是否发送短信
      SELECT COUNT(*) INTO V_EXISTS FROM UTB_SMS_CONFIG A WHERE A.WORKCODE = 'GA_ZGTQ_YCTQ_CG' AND A.ISUSED = 1;
      
      IF V_EXISTS > 0 THEN
        --获取发送短信内容
        V_SMSCONTEXT := PKG_SMS_SEND.UFN_CREATECONTEXT(IN_WORKID   => V_YWLSH, 
                                                       IN_WORKCODE => 'GA_ZGTQ_YCTQ_CG',
                                                       IN_YWMXID   => '');
        IF V_SMSCONTEXT IS NULL THEN
          ROLLBACK;
          OUT_MSG := '获取短信内容失败！';
          RETURN;
        END IF;
        
        SELECT A.INDVID, A.XINGMING, A.ZJHM INTO V_INDVID, V_XINGMING, V_ZJHM FROM UTB_GA_EXECMONEYDRAW A WHERE A.WORKSERIALNO = V_YWLSH;
        
        --获取手机号码
        V_PHONE := PKG_SMS_SEND.UFN_GETSMSSJHM(IN_CUSTTYPE => 0, 
                                               IN_CUSTID   => V_INDVID, 
                                               IN_YHMC     => V_XINGMING, 
                                               IN_YHBM     => V_ZJHM);
      
        IF V_PHONE IS NOT NULL THEN
          --保存短信内容到短信表
          PKG_SMS_SEND.UPR_SENDSMS(IN_WORKCODE => 'GA_ZGTQ_YCTQ_CG',
                                   IN_PHONE    => V_PHONE,
                                   IN_CONTEXT  => V_SMSCONTEXT);
        END IF;
        
      END IF;
    END IF;
    
    IF IN_TRADESTATE <> 1 AND V_WORKTYPECODE IN ('QYHD', 'ZFTH_TQ_QYHD') THEN
      --判断是否发送短信
      SELECT COUNT(*) INTO V_EXISTS FROM UTB_SMS_CONFIG A WHERE A.WORKCODE = 'DFL_QYZX_ZFSB' AND A.ISUSED = 1;
      
      IF V_EXISTS > 0 THEN
        --获取发送短信内容
        V_SMSCONTEXT := PKG_SMS_SEND.UFN_CREATECONTEXT(IN_WORKID   => TO_CHAR(V_EXTENDS), 
                                                       IN_WORKCODE => 'DFL_QYZX_ZFSB',
                                                       IN_YWMXID   => V_YWLSH);
        IF V_SMSCONTEXT IS NULL THEN
          ROLLBACK;
          OUT_MSG := '获取短信内容失败！';
          RETURN;
        END IF;
        
        SELECT A.CURINDVID, B.XINGMING, B.ZJHM INTO V_INDVID, V_XINGMING, V_ZJHM FROM UTB_GA_DFLPLANEXPLODE A 
        LEFT JOIN UTB_GA_INDVINFO B ON A.CURINDVID = B.INDVID WHERE TO_CHAR(A.EXPID) = V_YWLSH
        AND A.PLANID = (SELECT B.PLANID FROM UTB_GA_DFLPLANMAIN B WHERE B.PLANSERIAL = TO_CHAR(V_EXTENDS));
        
        --获取手机号码
        V_PHONE := PKG_SMS_SEND.UFN_GETSMSSJHM(IN_CUSTTYPE => 0, 
                                               IN_CUSTID   => V_INDVID, 
                                               IN_YHMC     => V_XINGMING, 
                                               IN_YHBM     => V_ZJHM);
      
        IF V_PHONE IS NOT NULL THEN
          --保存短信内容到短信表
          PKG_SMS_SEND.UPR_SENDSMS(IN_WORKCODE => 'DFL_QYZX_ZFSB',
                                   IN_PHONE    => V_PHONE,
                                   IN_CONTEXT  => V_SMSCONTEXT);
        END IF;
        
      END IF;
    END IF;
    
    IF V_WORKTYPECODE IN ('BZJTQ', 'ZFTH_TQ_BZJTQ') THEN
      IF IN_TRADESTATE = 1 THEN
        V_SMS_CODE := 'YH_BZJTQ_CG';
      ELSE
        V_SMS_CODE := 'YH_BZJTQ_SB';
      END IF;
      
      --判断是否发送短信
      SELECT COUNT(*) INTO V_EXISTS FROM UTB_SMS_CONFIG A WHERE A.WORKCODE = V_SMS_CODE AND A.ISUSED = 1;
      
      IF V_EXISTS > 0 THEN
        --获取发送短信内容
        V_SMSCONTEXT := PKG_SMS_SEND.UFN_CREATECONTEXT(IN_WORKID   => IN_YWLSH, 
                                                       IN_WORKCODE => V_SMS_CODE,
                                                       IN_YWMXID   => '');
        IF V_SMSCONTEXT IS NULL THEN
          ROLLBACK;
          OUT_MSG := '获取短信内容失败！';
          RETURN;
        END IF;
        
        SELECT A.DEVELOPERID INTO V_DEVELOPERID FROM UTB_MARGIN_MAIN A WHERE A.SERIALCODE = V_YWLSH;
        
        V_PHONE := PKG_DK_PUBLIC.UFN_DK_GETDEVELOPERSJHM(V_DEVELOPERID);
        
        IF V_PHONE IS NOT NULL THEN
          --保存短信内容到短信表
          PKG_SMS_SEND.UPR_SENDSMS(IN_WORKCODE => V_SMS_CODE,
                                   IN_PHONE    => V_PHONE,
                                   IN_CONTEXT  => V_SMSCONTEXT);
        END IF;
        
      END IF;
      
    END IF;
    
    IF IN_TRADESTATE = 1 THEN
      UPDATE UTB_YH_YHJSLSXX A SET A.PPZT = 0 WHERE A.FSE = -V_FSE AND TO_CHAR(A.JSFSRQ, 'YYYYMM') = TO_CHAR(SYSDATE, 'YYYYMM') AND A.ISPP = 0;
      PKG_YH_NOTICE.UPR_YH_ZDPP_BATCH;
    END IF;
    
    -- 业务交易结果回写
    
    -- 签约还贷
    IF V_WORKTYPECODE IN ('QYHD', 'ZFTH_TQ_QYHD') THEN
      UPDATE UTB_GA_DFLPLANEXPLODE A SET A.TXSTATE = CASE WHEN IN_TRADESTATE = 1 THEN 1 ELSE 2 END 
      WHERE A.EXPID = TO_NUMBER(V_YWLSH) AND A.PLANID = (SELECT B.PLANID FROM UTB_GA_DFLPLANMAIN B WHERE B.PLANSERIAL = TO_CHAR(V_EXTENDS));
    END IF;
    
    EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      OUT_MSG := SQLERRM;
      RETURN;
  END UPR_CALLBACKFUNDTRADE;
  
  --交易结果回写——批量
  PROCEDURE UPR_CALLBACKFUNDTRADEBATCH(IN_PLLSH IN VARCHAR2, IN_TRADESTATUS IN NUMBER, IN_DETAILXML IN CLOB, OUT_MSG OUT VARCHAR2)AS
    V_EXISTS         NUMBER(10);
    V_WORKTYPECODE   VARCHAR2(50 CHAR);
    V_TRADESTATE_PL  NUMBER(10);
    V_TRADEDATE_PL   DATE;
    V_YWAREAID       NUMBER(10);
    V_DEBANKACCID    NUMBER(10);
    V_FSE            NUMBER(18, 2) := 0;
    V_ZHAIYAO        VARCHAR2(500 CHAR);
    V_REMARK         VARCHAR2(500 CHAR);
    V_INDVID         NUMBER(20);
    V_EXTENDS        VARCHAR2(4000 CHAR);
    V_YWDZTZID       NUMBER(20);
    V_ISABNORMALTRADE NUMBER(10);
    V_TRADEMSG       VARCHAR2(500 CHAR);
    V_YWLSH          VARCHAR2(50 CHAR);
    V_MXLSHS         CLOB;
    V_FSE_DETAIL     NUMBER(18, 2) := 0;
    V_SMSCONTEXT     VARCHAR2(500 CHAR);
    V_PHONE          VARCHAR2(50 CHAR);
    V_ZJLSH          VARCHAR2(50 CHAR);
  BEGIN
    SELECT COUNT(*) INTO V_EXISTS FROM UTB_YH_FUNDTRADE A WHERE A.PLLSH = IN_PLLSH;
    
    IF V_EXISTS = 0 THEN
      OUT_MSG := '数据不存在';
      RETURN;
    END IF;
    
    SELECT A.WORKTYPECODE, A.TRADESTATE, A.TRADEDATE, A.FSE, A.DEBANKACCID, A.ZHAIYAO, A.YWAREAID, NVL(A.ISABNORMALTRADE, 0)
    INTO V_WORKTYPECODE, V_TRADESTATE_PL, V_TRADEDATE_PL, V_FSE, V_DEBANKACCID, V_REMARK, V_YWAREAID, V_ISABNORMALTRADE
    FROM UTB_YH_FUNDTRADE A WHERE A.PLLSH = IN_PLLSH;
    
    IF V_TRADESTATE_PL = 2 OR V_TRADESTATE_PL = 3 THEN
      OUT_MSG := '该笔交易结果已回写';
      RETURN;
    END IF;
    
    IF IN_TRADESTATUS = 1 THEN
      UPDATE UTB_YH_FUNDTRADE A SET A.TRADESTATE = 2 WHERE A.PLLSH = IN_PLLSH;
    ELSE
      UPDATE UTB_YH_FUNDTRADE A SET A.TRADESTATE = 3 WHERE A.PLLSH = IN_PLLSH;  
    END IF;
    
    FOR V_D IN (SELECT EXTRACTVALUE(A.XML, 'CrRow/MXLSH') AS MXLSH, TO_NUMBER(EXTRACTVALUE(A.XML, 'CrRow/TRADESTATE')) AS TRADESTATE,
      EXTRACTVALUE(A.XML, 'CrRow/TRADEMSG') AS TRADEMSG 
      FROM TABLE(PKG_SYS_XML.UFN_SYS_XMLSPLIT(IN_DETAILXML, 'RootNode/CrRow')) A) LOOP
      
      SELECT TO_CHAR(A.EXTENDS), A.YWLSH, A.FSE INTO V_EXTENDS, V_YWLSH, V_FSE_DETAIL FROM UTB_YH_FUNDTRADE_DETAIL A WHERE A.MXLSH = V_D.MXLSH AND A.PLLSH = IN_PLLSH;
      
      IF V_WORKTYPECODE = 'QYHD' THEN
        SELECT C.DWMC || ',' || B.XINGMING || '签约还贷支取', A.CURINDVID INTO V_ZHAIYAO, V_INDVID
        FROM UTB_GA_DFLPLANEXPLODE A LEFT JOIN UTB_GA_INDVINFO B ON A.CURINDVID = B.INDVID
        LEFT JOIN UTB_GA_ETPSINFO C ON B.ETPSID = C.ETPSID LEFT JOIN UTB_GA_DFLPLANMAIN D ON A.PLANID = D.PLANID
        WHERE TO_CHAR(A.EXPID) = V_YWLSH AND D.PLANSERIAL = V_EXTENDS; 
          
        --写入资金表数据
        PKG_GA_DFLPLANAPPLY_CHECK.UPR_GA_SAVEDFLTOFUND(IN_PLANSERIAL    => V_EXTENDS, 
                                                       IN_INDVID        => V_INDVID,
                                                       IN_CALINTERDATE  => V_TRADEDATE_PL,
                                                       OUT_FUNDSERIALNO => V_ZJLSH,
                                                       OUT_MSG          => OUT_MSG);
           
        IF OUT_MSG IS NOT NULL THEN
          ROLLBACK;
          RETURN;
        END IF;
        
        IF V_D.TRADESTATE <> 2 THEN
          --判断是否发送短信
          SELECT COUNT(*) INTO V_EXISTS FROM UTB_SMS_CONFIG A WHERE A.WORKCODE = 'DFL_QYZX_ZFSB' AND A.ISUSED = 1;
          
          IF V_EXISTS > 0 THEN
            --获取发送短信内容
            V_SMSCONTEXT := PKG_SMS_SEND.UFN_CREATECONTEXT(IN_WORKID   => TO_CHAR(V_EXTENDS), 
                                                           IN_WORKCODE => 'DFL_QYZX_ZFSB',
                                                           IN_YWMXID   => V_YWLSH);
            IF V_SMSCONTEXT IS NULL THEN
              ROLLBACK;
              OUT_MSG := '获取短信内容失败！';
              RETURN;
            END IF;
            
            SELECT A.CURINDVID INTO V_INDVID FROM UTB_GA_DFLPLANEXPLODE A WHERE TO_CHAR(A.EXPID) = V_YWLSH
            AND A.PLANID = (SELECT B.PLANID FROM UTB_GA_DFLPLANMAIN B WHERE B.PLANSERIAL = TO_CHAR(V_EXTENDS));
            
            --获取签约手机 CUSTTYPE:职工0，单位1，项目2  ISACTIVE:0 解约，1 签约 CUSTID:职工ID，单位ID，项目ID
            SELECT COUNT(*) INTO V_EXISTS FROM UTB_SMS_SIGN A WHERE A.CUSTTYPE = 0 AND A.ISACTIVE = 1 AND A.CUSTID = V_INDVID;
            
            IF V_EXISTS > 0 THEN
              SELECT A.PHONE INTO V_PHONE FROM UTB_SMS_SIGN A WHERE A.CUSTTYPE = 0 AND A.ISACTIVE = 1 AND A.CUSTID = V_INDVID;
              
              --保存短信内容到短信表
              PKG_SMS_SEND.UPR_SENDSMS(IN_WORKCODE => 'DFL_QYZX_ZFSB',
                                       IN_PHONE    => V_PHONE,
                                       IN_CONTEXT  => V_SMSCONTEXT);
            END IF;
            
          END IF;
        END IF;
      
        -- 业务状态回写
        UPDATE UTB_GA_DFLPLANEXPLODE A SET A.TXSTATE = CASE WHEN V_D.TRADESTATE = 2 THEN 1 ELSE 2 END
        WHERE TO_CHAR(A.EXPID) = V_YWLSH AND A.PLANID = (SELECT B.PLANID FROM UTB_GA_DFLPLANMAIN B WHERE B.PLANSERIAL = TO_CHAR(V_EXTENDS));
      END IF;
      
      IF V_WORKTYPECODE = 'SDQY' THEN
        SELECT C.DWMC || ',' || B.XINGMING || '商贷签约支取', A.CURINDVID INTO V_ZHAIYAO, V_INDVID
        FROM UTB_GA_DFBPLANEXPLODE A LEFT JOIN UTB_GA_INDVINFO B ON A.CURINDVID = B.INDVID
        LEFT JOIN UTB_GA_ETPSINFO C ON B.ETPSID = C.ETPSID LEFT JOIN UTB_GA_DFBPLANMAIN D ON A.PLANID = D.PLANID
        WHERE TO_CHAR(A.EXPID) = V_YWLSH AND D.PLANSERIAL = V_EXTENDS; 
          
        --写入资金表数据
        PKG_GA_DFBPLANAPPLY_CHECK.UPR_GA_SAVEDFBTOFUND(IN_PLANSERIAL    => V_EXTENDS, 
                                                       IN_INDVID        => V_INDVID,
                                                       IN_CALINTERDATE  => V_TRADEDATE_PL,
                                                       OUT_FUNDSERIALNO => V_ZJLSH,
                                                       OUT_MSG          => OUT_MSG);
           
        IF OUT_MSG IS NOT NULL THEN
          ROLLBACK;
          RETURN;
        END IF;
      END IF;
      
      UPDATE UTB_YH_FUNDTRADE_DETAIL A SET A.TRADESTATE = V_D.TRADESTATE, A.TRADEMSG = V_D.TRADEMSG, A.ZJLSH = V_ZJLSH WHERE A.MXLSH = V_D.MXLSH AND A.PLLSH = IN_PLLSH;
      
      PKG_YH_FUNDTRADELOG.UPR_SAVEFUNDTRADELOG(IN_YWAREAID      => 0, 
                                               IN_WORKTYPECODE  => V_WORKTYPECODE, 
                                               IN_YWLSH         => V_YWLSH, 
                                               IN_MXLSH         => V_D.MXLSH, 
                                               IN_FSE           => V_FSE_DETAIL, 
                                               IN_DEBANKACCID   => V_DEBANKACCID, 
                                               IN_ZHAIYAO       => V_D.TRADEMSG, 
                                               IN_LCMC          => '支付申请-交易',  
                                               IN_OPTNAME       => 'system',  
                                               IN_OPTCODE       => '',  
                                               OUT_MSG          => OUT_MSG);
                                                   
      IF OUT_MSG IS NOT NULL THEN
        ROLLBACK;
        RETURN;
      END IF;
      
      V_TRADEMSG := V_D.TRADEMSG;
    END LOOP;
    
    --写入业务动账通知表数据  
    PKG_CN_MATCH.UPR_CN_INTERFACE_YWDZTZ(IN_FUNDTYPE     => 0,
                                         IN_WORKTYPECODE => V_WORKTYPECODE, 
                                         IN_YWLSH        => IN_PLLSH, 
                                         IN_JZRQ         => V_TRADEDATE_PL,
                                         IN_FSE          => V_FSE,
                                         IN_ZHAIYAO      => V_REMARK, 
                                         IN_BANKACCID    => V_DEBANKACCID, 
                                         IN_OPTNAME      => '系统', 
                                         IN_AREAID       => V_YWAREAID,
                                         OUT_YWDZTZID    => V_YWDZTZID,
                                         OUT_MSG         => OUT_MSG);
    IF OUT_MSG IS NOT NULL THEN
      ROLLBACK;
      RETURN;
    END IF;
    
    IF IN_TRADESTATUS = -1 THEN
      IF V_ISABNORMALTRADE = 0 THEN
        SELECT LISTAGG(A.MXLSH, ',') WITHIN GROUP (ORDER BY A.MXLSH) INTO V_MXLSHS FROM UTB_YH_FUNDTRADE_DETAIL A WHERE A.PLLSH = IN_PLLSH;  
      
        PKG_YH_FUNDTRADE_ABNORMAL.UPR_SAVEFUNDTRADE_ABNORMAL(IN_PLLSH      => IN_PLLSH, 
                                                             IN_MXLSHS     => V_MXLSHS, 
                                                             IN_TRADEMSG   => V_TRADEMSG, 
                                                             IN_INFOSOURCE => 0, 
                                                             IN_EXTENDS    => '', 
                                                             IN_OPTNAME    => '系统', 
                                                             IN_OPTCODE    => '', 
                                                             OUT_MSG       => OUT_MSG);
        IF OUT_MSG IS NOT NULL THEN
          ROLLBACK;
          RETURN;
        END IF;
          
        UPDATE UTB_YH_FUNDTRADE A SET A.ISABNORMALTRADE = 1 WHERE A.PLLSH = IN_PLLSH;
      END IF;
      
      UPDATE UTB_CN_YWDZTZ A SET A.PPZT = 1, A.PPRQ = SYSDATE, A.PPOPTNAME = '系统' WHERE A.YWDZTZ_ID = V_YWDZTZID;  
    END IF;
    
    EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      OUT_MSG := SQLERRM;
      RETURN;
  END UPR_CALLBACKFUNDTRADEBATCH;
  
  
END PKG_YH_FUNDTRADE_TRANSACTION;
/
