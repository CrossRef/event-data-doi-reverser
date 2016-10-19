(ns event-data-doi-reverser.url-test
  (:require [clojure.test :refer :all]
            [event-data-doi-reverser.urls :as urls]))

(deftest jsessionid-test
  (testing "JSESSIONID is removed"

    ; Remove at the end.
    (is (= "http://onlinelibrary.wiley.com/doi/10.1002/14356007.b04_477.pub2/abstract"
       (urls/remove-session-id "http://onlinelibrary.wiley.com/doi/10.1002/14356007.b04_477.pub2/abstract;jsessionid=DD9AD0EF3745D55704C00E31796F0166.f04t03")))

    ; Can't remove, it's stuck in the middle.
    (is (= "http://jpharmsci.org/action/consumeSharedSessionAction?SERVER=WZ6myaEXBLFhx%2B6Ws3Nrug%3D%3D&MAID=5ovq0AmgcuyAzuGD%2F%2FTyag%3D%3D&JSESSIONID=aaa__0LVu3h1M5_kK9uFv&ORIGIN=432061129&RD=RD"
       (urls/remove-session-id "http://jpharmsci.org/action/consumeSharedSessionAction?SERVER=WZ6myaEXBLFhx%2B6Ws3Nrug%3D%3D&MAID=5ovq0AmgcuyAzuGD%2F%2FTyag%3D%3D&JSESSIONID=aaa__0LVu3h1M5_kK9uFv&ORIGIN=432061129&RD=RD")))

    ; Remove mixed case
    (is (= "http://iopscience.iop.org/book/978-0-7503-1218-9/chapter/bk978-0-7503-1218-9ch16"
       (urls/remove-session-id "http://iopscience.iop.org/book/978-0-7503-1218-9/chapter/bk978-0-7503-1218-9ch16;jsessionid=E616FFAEA824F58AA3F99A42CD451B79.c2.iopscience.cld.iop.org")))

    ; Can't remove, it's difficult to reliably detect.
    (is (= "http://pt.wkhealth.com/pt/re/lwwgateway/landingpage.htm;jsessionid=YGZTx04hzjJQ3tv92rcQvBQltPywtWmR3w83jNx3hmpKLJtncF71!-1552860756!181195628!8091!-1?sid=WKPTLP:landingpage&an=00003643-200502000-00002"
       (urls/remove-session-id "http://pt.wkhealth.com/pt/re/lwwgateway/landingpage.htm;jsessionid=YGZTx04hzjJQ3tv92rcQvBQltPywtWmR3w83jNx3hmpKLJtncF71!-1552860756!181195628!8091!-1?sid=WKPTLP:landingpage&an=00003643-200502000-00002")))))