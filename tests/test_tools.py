# The MIT License (MIT) # Copyright (c) 2014-2017 University of Bristol
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
#  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
#  DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
#  OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
#  OR OTHER DEALINGS IN THE SOFTWARE.

import unittest

from hyperstream import HyperStream, TimeInterval
from helpers import *

RANDOM_VALUES = {
    'betavariate': [0.03998183409782358, 0.9813044943431838, 0.10380855496305717, 0.13844628987627133,
                    0.15584779015201922, 0.9361654361179801, 0.6917772977096855, 0.2027117945505411,
                    0.43918758126298996, 0.8544113252370211, 0.01287142290500471, 0.4457116661227356,
                    0.17361201107242924, 0.29421359793778146, 0.5717544881170562, 0.4142076903024753,
                    0.9528294712121721, 0.5548663306968494, 0.8535977478416558, 0.7265488271462255, 0.43456685968408554,
                    0.8457282928115185, 0.7556507799910569, 0.9676437171376849, 0.8552461324633835, 0.6226682409295989,
                    0.7154181886502158, 0.11057504812003682, 0.9757826222032523, 0.9113396858109933, 0.7704100554930415,
                    0.49463201261406287, 0.21451900182038908, 0.24525336526993044, 0.6153957180309173,
                    0.7688499478837174, 0.14394387349955903, 0.5536015578637701, 0.28090140753005033,
                    0.7725517039478933, 0.7823279736771419, 0.48633348571409923, 0.7508031078451823,
                    0.10226659410181979, 0.06853557078135092, 0.9147747711820725, 0.5867691193112203,
                    0.23332000926901225, 0.23379728552153975, 0.27673711204521456, 0.2183061140821174,
                    0.16281664408091343, 0.9531146017422517, 0.666718604507751, 0.7542864877292227, 0.6337360989124363,
                    0.7453225805749533, 0.7756013764371857, 0.8698431444664898, 0.6525896922649966],
    'expovariate': [3.3948238068746592, 0.581127564523488, 0.007519672058314841, 2.4188488608750407, 2.8013009594734526,
                    0.8728184278703051, 1.1134117062707574, 0.08767147863106146, 1.454491546927796, 0.27024796731991774,
                    0.03129875746448713, 1.5548205557405708, 0.4247839698013932, 0.9762569885332901, 0.956632880871021,
                    0.16081994853456713, 0.20222714181985582, 0.12150454278154425, 0.014726687798474513,
                    0.6669952247529831, 3.349598620046617, 0.06674071055335702, 0.7788972127637638, 0.6271694936589802,
                    0.9199560649103441, 0.09313426182154552, 0.865128825812668, 0.3141431029917651, 0.8129054269127287,
                    1.034607693670155, 0.6559214779762201, 0.43887580216844324, 0.28655220613946686, 2.7107864117498326,
                    0.6040160874636319, 0.7553656365426483, 0.019488234358730122, 0.7094837501708041,
                    0.005797007645201512, 0.1552144109168155, 0.6402263694565866, 0.4737666056051601,
                    0.05569792649631533, 0.8855881862164247, 0.17913052205554167, 0.8149312439067005,
                    0.15577200199556818, 2.769506831287718, 1.4739458186400172, 3.1450010773061314, 0.15225154281602005,
                    0.364408641013038, 0.04039461109902167, 0.32404692423975834, 1.6425423945210784, 0.1952159759825008,
                    0.16791051413423777, 3.0948601569765564, 0.16788790502727166, 1.795100810202958],
    'gammavariate': [0.034122056303517126, 0.8193169385167471, 4.8939902312070505, 0.09323876796587531,
                     0.0626533686333073, 0.5408938886770757, 0.39814673064728184, 2.477674145470838, 0.2659454566589995,
                     1.4404980957300801, 3.479785441733883, 0.23727706397407544, 1.061059410972392, 0.4727570563868493,
                     0.48480755647711316, 1.906802450982214, 1.6977739077437124, 2.1679408350578795, 4.225448242167752,
                     0.7200014696505875, 0.035729192259713154, 2.7401249245233514, 0.6141730087038979,
                     0.7637873530847323, 0.5083895108982366, 2.4199188921772943, 0.5464481620634848, 1.3108696706052976,
                     0.5862093005943696, 0.4390722018297163, 0.7318123955885246, 1.0349640615412754, 1.389691639897196,
                     0.06879771405745859, 0.7910069649973595, 0.6345741547003236, 3.9476726543229006, 0.677073210230625,
                     5.153310521407328, 1.9395514155512064, 0.7490258658074364, 0.9745889223794751, 2.9155320648171457,
                     0.5318305096626815, 1.8078691943879561, 0.5845973288509296, 1.9362370336392714,
                     0.06474431863645083, 0.2600928830327794, 0.044021772573587796, 1.9573813414076051,
                     1.1861567624233778, 3.229188209498897, 1.2845189630730334, 0.21503587612405725, 1.7296693695894334,
                     1.8671048808707489, 0.046338583263660596, 1.8672285513118005, 0.18165462626009457],
    'gauss': [1.0542196419272387, -0.22555725575068641, 2.1970405483761803, 0.1034917897273693, 1.2261975296244925,
              -0.4920126767507134, -0.19811499390451837, -0.3689083984573075, 0.07599437119158767, -0.7312460531086856,
              1.7304697908468525, 0.3392866258012423, -0.7933015653304946, 1.1502984845294577, -0.42348499853186244,
              -0.3772271902814042, 0.20119116396015987, 0.4500346665618601, 1.1501162307205914, 0.10593915866678266,
              0.3565028927584915, -0.07991938789518833, -1.0828570598554803, -0.2859363132571751, -0.34681496932144434,
              -0.2568811022587233, -0.6969793664130848, -0.37749962752027333, -1.3489944442044972, -0.4994290508627922,
              -0.9302412877080083, 0.11136763883746918, 0.012404354114906677, 2.328394072209139, -1.1767807069691938,
              0.35484988486736313, 1.182456826105767, 0.14409495042326753, 0.5567939836959213, 0.020230708184767448,
              -0.9592601501987155, 0.16538795437110324, 1.254495799595116, 0.44431594753177583, 0.6567425490859827,
              1.0947838654426025, 1.4512805432247062, 1.8527812735001499, 0.3296489194417435, -2.4862288198239426,
              0.5390833969382259, 0.6619715803353036, 0.7802644950576452, 0.19819476842730915, 0.2172363060691118,
              -0.5858671686401694, 1.40396327888095, 2.053924883121816, 1.0694388367436392, 1.5641298523045737],
    'lognormvariate': [4.182149649506653, 6.072683633963685, 1.3789060522879304, 1.820308315387552, 0.49614300391442684,
                       1.2628237848204944, 0.5412329224285202, 0.19742790421033787, 1.1410795476211322,
                       1.21052425268962, 1.2038851594104196, 1.3131490562653974, 0.9507949104741118, 0.8435004371871028,
                       0.18703205831480474, 0.9278668322481841, 0.15657049210791776, 0.27195301019062196,
                       0.41226505959615606, 0.3355008541092324, 1.894937540967587, 0.2772979518452522,
                       0.6757266576966814, 2.81775997400662, 2.502088419825137, 0.6229390116838252, 2.152480312223666,
                       0.4527920267013729, 0.8828731946078828, 1.3494442377522704, 0.869975688840489,
                       2.1115602189171807, 0.4097250156244801, 1.8792342300327505, 2.606256173070972, 6.229504939045913,
                       0.9062354977700294, 0.8280165270060298, 0.6829152847147666, 1.0127064789865667,
                       0.3670987657221395, 0.2789108252374628, 0.4284405018934631, 1.0049070179321764,
                       0.18556528338818723, 1.0905363587891013, 0.2610124042693406, 0.16250565858873428,
                       1.1249379083434752, 2.8285199611394467, 0.720248041371186, 17.481108541304152, 2.289963461381308,
                       1.6811433658093013, 0.42494073258082027, 1.668947117554719, 2.447378048054388,
                       2.2586354440513916, 0.6537953162760484, 0.3889722442448],
    'normalvariate': [1.430825384561959, 1.8038006216944658, 0.32129046907046666, 0.5990058908005511,
                      -0.7008910794682004, 0.2333503125104609, -0.6139055522583281, -1.6223818032579733,
                      0.13197478590915163, 0.1910535324613778, 0.18555395978793937, 0.2724281123087385,
                      -0.0504568963984016, -0.17019485869500034, -1.676475241982295, -0.07486705622556843,
                      -1.8542489416099452, -1.3021259842624142, -0.8860887879582023, -1.0921307767935147,
                      0.6391858780796363, -1.282662712448799, -0.39196663723139924, 1.0359422338397999,
                      0.9171257510786165, -0.47330665955516893, 0.7666208108080782, -0.7923223611232617,
                      -0.12457369615630783, 0.2996928319495584, -0.13929001158545146, 0.7474271144818401,
                      -0.8922690378981809, 0.6308643694120554, 0.9579147751265712, 1.8292968656043538,
                      -0.09845607540195042, -0.1887221646438687, -0.381384461196793, 0.012626429070964904,
                      -1.0021243507237927, -1.2768631710558196, -0.8476034026424042, 0.00489501776037762,
                      -1.684348526836401, 0.08666964749676666, -1.3431873468525943, -1.817042455734101,
                      0.11772784154963907, 1.0397535929228738, -0.328159624394767, 2.861120785911318,
                      0.8285358617101678, 0.5194741368155915, -0.8558055725331704, 0.5121929590696971,
                      0.8950172670327545, 0.8147608451141645, -0.42496094856588496, -0.9442472894651183],
    'paretovariate': [29.80940080071751, 1.7880534401352315, 1.0075480157929015, 11.232921214436221, 16.46605451238403,
                      2.3936476787662757, 3.044728413668828, 1.091629439516145, 4.282305560549401, 1.3102893193840228,
                      1.0317937139180504, 4.73423691729549, 1.529260017630925, 2.654501793093326, 2.602917368708495,
                      1.174473484103452, 1.2241260267487817, 1.1291944955974207, 1.014835659740027, 1.9483740899647228,
                      28.491295513537708, 1.0690182570835713, 2.1790678917276303, 1.8723035050106858, 2.509180146459839,
                      1.0976090923717308, 2.375312067774983, 1.3690856427811218, 2.2544486160976094, 2.814002068228733,
                      1.926917311398427, 1.5509626492274813, 1.331827695661731, 15.041099362328692, 1.8294513029924269,
                      2.1283895981144103, 1.0196793696084077, 2.0329414820037095, 1.0058138428094914,
                      1.1679083465608209, 1.8969102332455021, 1.6060321043903114, 1.057278259745987, 2.4244099767209266,
                      1.196176861442035, 2.259020345596285, 1.1685597434253272, 15.950765651183254, 4.3664303364032895,
                      23.21970048799658, 1.1644531094097856, 1.4396623990728115, 1.0412215707229704, 1.3827121883154585,
                      5.1682926620010985, 1.2155734928205928, 1.1828307592772367, 22.084150007288745, 1.18280401683239,
                      6.020081576863732],
    'randint': [5, 3, 1, 5, 5, 3, 4, 1, 4, 2, 1, 4, 2, 4, 4, 1, 1, 1, 1, 3, 5, 1, 3, 3, 4, 1, 3, 2, 3, 4, 3, 2, 2, 5, 3,
                3, 1, 3, 1, 1, 3, 2, 1, 3, 1, 3, 1, 5, 4, 5, 1, 2, 1, 2, 5, 1, 1, 5, 1, 5],
    'random': [
        0.9664535356921388, 0.4407325991753527, 0.007491470058587191, 0.9109759624491242, 0.939268997363764,
        0.5822275730589491, 0.6715634814879851, 0.08393822683708396, 0.7664809327917963, 0.23680977536311776,
        0.030814021726609964, 0.7887727172362835, 0.3460889655971231, 0.6232814750391685, 0.6158156951036152,
        0.14855463870828756, 0.18309064740993164, 0.11441296968868764, 0.014618780486909122, 0.48675154060475834,
        0.9649015609162157, 0.06456228097718608, 0.5410881855511303, 0.46589855900830957, 0.6014634495610515,
        0.08892882999066232, 0.5790026861873665, 0.26958550381944824, 0.5564325605562156, 0.6446342341782827,
        0.48103637136651844, 0.35523914744298335, 0.249152121361209, 0.9335154980423467, 0.45338801947649354,
        0.5301612069115903, 0.019299566309716853, 0.5081019257797922, 0.005780237417743139, 0.14376842759559538,
        0.47282692534740633, 0.3773474407725964, 0.05417519864614284, 0.5875285081310694, 0.1640032237419612,
        0.5573302374414681, 0.1442457216019083, 0.9373070846962247, 0.7709799715197749, 0.9569331223494054,
        0.14122776441649953, 0.3053927082876986, 0.03958962422796164, 0.27678369479169207, 0.8065125051156038,
        0.177343035278254, 0.15457051471078964, 0.9547186557023949, 0.154551400089043, 0.8338892941512318
    ],
    'randrange': [18, 14, 10, 18, 18, 14, 16, 10, 16, 12, 10, 16, 12, 16, 16, 10, 10, 10, 10, 14, 18, 10, 14, 14, 16,
                  10, 14, 12, 14, 16, 14, 12, 12, 18, 14, 14, 10, 14, 10, 10, 14, 12, 10, 14, 10, 14, 10, 18, 16, 18,
                  10, 12, 10, 12, 18, 10, 10, 18, 10, 18],
    'triangular': [4.682762875874238, 3.6261597692269096, 2.212011368448777, 4.483209798223082, 4.57315927102875,
                   3.8690546911082335, 4.007372398360772, 2.709668486705239, 4.163007048043646, 3.1919977567842595,
                   2.429981546533871, 4.203958639082648, 3.441018318267585, 3.933827513051516, 3.922210750833969,
                   2.944101600596951, 3.048114442443949, 2.828539569442598, 2.2961632707164323, 3.70894974871368,
                   4.6755076006262195, 2.622393513673718, 3.8018127298103934, 3.6719423895726364, 3.8996791037873497,
                   2.7304607997312202, 3.863871271607618, 3.271814854024236, 3.8271823563446787, 3.9666736905418998,
                   3.6988873500615367, 3.4599434525548927, 3.2226662374365516, 4.553397821464158, 3.6493417222816387,
                   3.78352663043464, 2.3402901671490097, 3.7460273636683796, 2.186229494190525, 2.928768305646555,
                   3.6843282198207206, 3.5046875571478546, 2.5701326090278096, 3.8775438873130015, 2.991977490899752,
                   3.8286556331493387, 2.930308728117419, 4.566319534782433, 4.171109123321607, 4.6405551044293665,
                   2.9205251688568854, 3.3536455406516845, 2.4873784416321363, 3.288682338185075, 4.238119113868061,
                   3.031531973168803, 2.9630280828017104, 4.631429744970086, 2.962968535588914, 4.2940735749766095],
    'uniform': [4.899360607076416, 3.322197797526058, 2.0224744101757617, 4.732927887347373, 4.817806992091292,
                3.7466827191768473, 4.014690444463955, 2.2518146805112518, 4.299442798375389, 2.7104293260893533,
                2.0924420651798297, 4.3663181517088505, 3.0382668967913693, 3.8698444251175057, 3.847447085310846,
                2.445663916124863, 2.5492719422297947, 2.343238909066063, 2.043856341460727, 3.460254621814275,
                4.894704682748647, 2.1936868429315584, 3.6232645566533908, 3.3976956770249287, 3.8043903486831545,
                2.266786489971987, 3.7370080585620995, 2.808756511458345, 3.6692976816686467, 3.933902702534848,
                3.4431091140995553, 3.06571744232895, 2.747456364083627, 4.80054649412704, 3.3601640584294805,
                3.590483620734771, 2.057898698929151, 3.5243057773393764, 2.0173407122532296, 2.431305282786786,
                3.418480776042219, 3.132042322317789, 2.1625255959384284, 3.762585524393208, 2.4920096712258837,
                3.6719907123244044, 2.432737164805725, 4.8119212540886735, 4.312939914559324, 4.870799367048216,
                2.4236832932494985, 2.916178124863096, 2.118768872683885, 2.830351084375076, 4.4195375153468115,
                2.532029105834762, 2.463711544132369, 4.864155967107185, 2.463654200267129, 4.501667882453695],
    'vonmisesvariate': [3.3578560548268355, 5.157489756582599, 4.479739182645804, 1.9079711175236223,
                        5.0551416045457955, 6.107096891938155, 2.9153877210293566, 5.460182454227138, 1.116271426329938,
                        4.960737570079505, 5.887441356819019, 0.9813164927560798, 6.274362166448599, 0.6325215286073413,
                        6.028546694550388, 3.418698935619459, 0.06048917899897393, 0.2761544776887228, 6.04368301527376,
                        5.632838807299208, 2.0455673875719, 0.8345023860519942, 0.7634712409224359, 5.805628236451655,
                        0.7556721076026238, 2.112922851993352, 4.920322771565935, 4.749565803242515, 3.723684053158859,
                        4.352031602899314, 5.614316866936048, 6.225137698980708, 5.766737554488466, 5.873170966841112,
                        0.3162772347943061, 2.4362970883235815, 0.6006385525933391, 1.532669183971546,
                        0.4885887018807995, 0.9035380398024331, 0.5477760557495457, 6.176392770219052,
                        1.4281202149258818, 6.053689328434808, 1.672377583279894, 0.6402668654539713,
                        5.2825043654518815, 1.467161497219122, 6.140657913249537, 4.016127529818116, 0.6285209774914348,
                        1.1412782771156924, 5.5175917888682235, 5.783409984284813, 5.543137983515277, 5.603426529843089,
                        5.666252112039157, 5.331163516794262, 1.4050214219138821, 4.65761503203891],
    'weibullvariate': [3.3948238068746592, 0.581127564523488, 0.007519672058314841, 2.4188488608750407,
                       2.8013009594734526, 0.8728184278703051, 1.1134117062707574, 0.08767147863106146,
                       1.454491546927796, 0.27024796731991774, 0.03129875746448713, 1.5548205557405708,
                       0.4247839698013932, 0.9762569885332901, 0.956632880871021, 0.16081994853456713,
                       0.20222714181985582, 0.12150454278154425, 0.014726687798474513, 0.6669952247529831,
                       3.349598620046617, 0.06674071055335702, 0.7788972127637638, 0.6271694936589802,
                       0.9199560649103441, 0.09313426182154552, 0.865128825812668, 0.3141431029917651,
                       0.8129054269127287, 1.034607693670155, 0.6559214779762201, 0.43887580216844324,
                       0.28655220613946686, 2.7107864117498326, 0.6040160874636319, 0.7553656365426483,
                       0.019488234358730122, 0.7094837501708041, 0.005797007645201512, 0.1552144109168155,
                       0.6402263694565866, 0.4737666056051601, 0.05569792649631533, 0.8855881862164247,
                       0.17913052205554167, 0.8149312439067005, 0.15577200199556818, 2.769506831287718,
                       1.4739458186400172, 3.1450010773061314, 0.15225154281602005, 0.364408641013038,
                       0.04039461109902167, 0.32404692423975834, 1.6425423945210784, 0.1952159759825008,
                       0.16791051413423777, 3.0948601569765564, 0.16788790502727166, 1.795100810202958]
}

SEA_ICE_SUMS = [
    15.48, 16.15, 17.259999999999998, 18.21, 19.38, 18.61, 18.869999999999997, 18.8, 20.380000000000003, 20.82, 17.96,
    15.88, 15.16, 15.89, 17.89, 19.21, 19.47, 18.78, 19.1, 18.84, 21.4, 20.54, 17.67, 15.66, 15.3, 15.8, 17.67, 18.9,
    19.79, 19.62, 19.43, 19.41, 21.45, 20.78, 17.66, 15.28, 14.84, 15.860000000000001, 17.77, 19.15, 19.479999999999997,
    18.8, 18.48, 19.14, 21.07, 21.21, 17.9, 16.060000000000002, 15.63, 16.6, 18.22, 19.72, 20.25, 19.88,
    19.490000000000002, 19.6, 21.15, 21.29, 18.38, 16.34, 15.420000000000002, 16.02, 17.67, 18.990000000000002,
    19.229999999999997, 18.52, 18.77, 18.8, 19.380000000000003, 20.33, 18.05, 15.5, 14.59, 15.39, 17.51, 19.79, 20.78,
    20.15, 19.450000000000003, 20.03, 21.34, 19.84, 17.0, 14.950000000000001, 15.02, 15.47, 17.240000000000002,
    18.990000000000002, 19.259999999999998, 19.07, 19.03, 19.33, 20.4, 20.490000000000002, 18.03, 15.64, 15.51, 16.14,
    17.8, 19.17, 19.36, 18.94, 18.59, 19.0, 21.1, 20.58, 17.46, 15.53, 15.08, 15.98, 18.13, 19.6, 19.92, 19.46, 18.82,
    18.75, 20.9, 20.61, 17.55, 15.010000000000002, 14.649999999999999, 15.7, 17.740000000000002, 19.65,
    19.939999999999998, 19.2, 19.26, 19.29, 20.8, 20.520000000000003, 17.67, 15.82, 15.52, 16.87, 18.21,
    19.240000000000002, 19.48, 18.71, 18.53, 18.82, 19.83, 19.9, 16.65, 15.0, 15.11, 15.649999999999999, 16.84, 18.2,
    18.73, 18.759999999999998, 17.880000000000003, 17.98, 19.93, 20.240000000000002, 18.05, 16.22, 15.76, 16.52, 17.86,
    19.17, 20.12, 19.14, 18.1, 18.060000000000002, 19.71, 19.86, 17.77, 15.879999999999999, 15.149999999999999, 16.03,
    17.41, 18.95, 19.98, 19.5, 18.72, 18.86, 20.38, 20.84, 17.67, 14.86, 14.18, 15.33, 17.28, 18.7, 18.939999999999998,
    18.44, 18.0, 18.79, 19.63, 20.42, 16.63, 14.24, 14.0, 14.379999999999999, 16.17, 17.47, 18.83, 18.5,
    18.689999999999998, 19.06, 20.32, 19.57, 16.38, 14.66, 14.41, 14.97, 16.37, 18.150000000000002, 18.54, 17.55, 17.05,
    18.07, 18.71, 20.03, 18.39, 15.8, 15.13, 16.79, 18.56, 19.34, 19.78, 18.5, 17.39, 17.2, 19.4, 20.509999999999998,
    17.75, 15.24, 14.71, 15.979999999999999, 18.4, 19.4, 19.83, 18.66, 18.08, 18.34, 19.04, 19.62, 17.04, 14.75,
    14.549999999999999, 15.69, 17.31, 18.67, 19.490000000000002, 18.81, 18.32, 17.26, 19.39, 19.810000000000002,
    16.689999999999998, 14.25, 14.120000000000001, 14.58
]


def rng_helper(tester, hs, ticker, ti, func, **kwargs):
    random = hs.channel_manager.memory.get_or_create_stream(func)
    tool = getattr(hs.plugins.data_generators.tools, func)
    tool(seed=1234, **kwargs).execute(sources=[], sink=random, interval=ti, alignment_stream=ticker)
    values = random.window().values()
    print(values)
    tester.assertListEqual(values, RANDOM_VALUES[func])


class TestTools(unittest.TestCase):
    def test_data_generators(self):
        hs = HyperStream()

        ti = TimeInterval.now_minus(minutes=1)

        M = hs.channel_manager.memory

        # Create a clock stream to align the random numbers to
        ticker = M.get_or_create_stream("ticker")
        hs.tools.clock().execute(sources=[], sink=ticker, interval=ti)

        # Test random number generators
        rng_helper(self, hs, ticker, ti, "betavariate", alpha=1.0, beta=1.0)
        rng_helper(self, hs, ticker, ti, "expovariate", lambd=1.0)
        rng_helper(self, hs, ticker, ti, "gammavariate", alpha=1.0, beta=1.0)
        rng_helper(self, hs, ticker, ti, "gauss")
        rng_helper(self, hs, ticker, ti, "lognormvariate", mu=0.0, sigma=1.0)
        rng_helper(self, hs, ticker, ti, "normalvariate", mu=0.0, sigma=1.0)
        rng_helper(self, hs, ticker, ti, "paretovariate", alpha=1.0)
        rng_helper(self, hs, ticker, ti, "randint", a=1, b=5)
        rng_helper(self, hs, ticker, ti, "random")
        rng_helper(self, hs, ticker, ti, "randrange", start=10, stop=20, step=2)
        rng_helper(self, hs, ticker, ti, "triangular", low=2, high=5, mode=4)
        rng_helper(self, hs, ticker, ti, "uniform", a=2, b=5)
        rng_helper(self, hs, ticker, ti, "vonmisesvariate", mu=0.0, kappa=1.0)
        rng_helper(self, hs, ticker, ti, "weibullvariate", alpha=1.0, beta=1.0)

        # Test custom random function

    def test_data_importers(self):
        hs = HyperStream()
        reader = hs.plugins.data_importers.tools.csv_reader('plugins/data_importers/data/sea_ice.csv')
        ti = TimeInterval(datetime(1990, 1, 1).replace(tzinfo=UTC), datetime(2011, 4, 1).replace(tzinfo=UTC))

        # TODO: More complicated tests, including headers, different delimiters, messy data etc etc.
        sea_ice = hs.channel_manager.memory.get_or_create_stream("sea_ice")

        reader.execute(sources=[], sink=sea_ice, interval=ti)

        sea_ice_sums = hs.channel_manager.mongo.get_or_create_stream("sea_ice_sums")
        hs.tools.list_sum().execute(sources=[sea_ice], sink=sea_ice_sums, interval=ti)

        print(sea_ice_sums.window().values())

        self.assertListEqual(sea_ice_sums.window().values(), map(sum, sea_ice.window().values()))
