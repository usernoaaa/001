
    @Synchronized
    public List<GiftTheCardRes> tapCard(TheCardReq theCardReq) {

        Customer customer = customerService.findById(ShiroUtils.getCustomerId());
        Integer cardTypeId = theCardReq.getType();
        TheCardType cardType = cardTypeService.findById(cardTypeId);
        Assert.notNull(CARD_TYPE_ERROR, cardType);

        if (!(theCardReq.getNum() == 1 || theCardReq.getNum() == 10 || theCardReq.getNum() == 100)) {
            Assert.isTrue(MarkCode.build("抽奖次数非法"), false);
        }

        // 真正的消费金额  该许愿神灯翻一次的价格*翻的次数
        BigDecimal realConsumeCoin = cardType.getCoin().multiply(new BigDecimal(theCardReq.getNum().toString()));

        AssetInfo assetInfo = customer.getAssetInfo();
        Assert.notNull(MarkCode.build("用户信息异常"), assetInfo);
        if (assetInfo.getCoin().compareTo(realConsumeCoin) < 0) {
            Assert.notNull(MarkCode.build("您的金币不足"), null);
        } else {
            logger.info(customer.getId() + "砸蛋消费金币" + realConsumeCoin.toString() + "当前用户金币" + assetInfo.getCoin());
        }

        // 消费金币计算金额
        Assert.isTrue(MarkCode.build("您的金币不足"), customerService.consumeCoinCard(customer, realConsumeCoin, theCardReq.getRoomId()));

        List<GiftTheCardRes> result = new ArrayList<>();
        Map<Integer, Temp> tapEggGift = new HashMap<>();
        List<Node> tagNodeList = new ArrayList<>();
        int tagNum = 0;

        boolean flagPersonPool = customer.isFlagPersonPool();

        List<GiftCompensation> giftCompensations = giftCompensationService.listCompensationByCondition(cardTypeId, customer.getId());
        if (giftCompensations != null && giftCompensations.size() > 0) {
            //获取当前用户抽奖次数
            // 抽奖次数等于补偿礼物次数不必去奖池抽奖了
            if (theCardReq.getNum() == giftCompensations.size()) {
//                logger.info("抽奖次数等于补偿礼物次数不必去奖池抽奖了");
                List<Node> nodes = new ArrayList<>();
                for (GiftCompensation compensation : giftCompensations) {
                    nodes.add(new Gson().fromJson(compensation.getNodeJson(), Node.class));
                    compensation.setStatus(true);
                    compensation.setCompensationTime(new Date());
                    giftCompensationService.save(compensation);
                }
                handlePoolPersonData(cardTypeId, nodes, cardType.getCoin(), nodes.size());

                Map<Integer, Temp> map = new HashMap<>(16);
                for (Node node : nodes) {
                    Temp temp = map.get(node.getGiftId());
                    if (Objects.isNull(temp)) {
                        temp = new Temp(1, node.getSend());
                        map.put(node.getGiftId(), temp);
                    } else {
                        temp.setNum(temp.getNum() + 1);
                    }
                }
                tapEggGift = map;

            } else if (theCardReq.getNum() > giftCompensations.size()) {
//                logger.info("抽奖次数大于补偿礼物次数 计算要去奖池抽奖的次数");
                tagNum = theCardReq.getNum() - giftCompensations.size();
                // 获取随机砸蛋奖品
                // tapEggGift = getTheCardGift(num, ROOM_POOL_CARD_CUSTOMER + customer.getId() + ":" + cardTypeId, ROOM_POOL_CARD_PUBLIC + cardTypeId, flagPersonPool, cardTypeId);

                if (!flagPersonPool) {
                    tagNodeList.addAll(tagEgg(customer.getId(), cardTypeId, cardType.getCoin(), new BigDecimal(tagNum)));
                } else {
                    tagNodeList.addAll(tagEggPerson2(customer.getId(), cardTypeId, cardType.getCoin(), new BigDecimal(tagNum)));
                }

                List<Node> nodes = new ArrayList<>();
                for (GiftCompensation compensation : giftCompensations) {
                    nodes.add(new Gson().fromJson(compensation.getNodeJson(), Node.class));
                    compensation.setStatus(true);
                    compensation.setCompensationTime(new Date());
                    giftCompensationService.save(compensation);
                }
                handlePoolPersonData(cardTypeId, nodes, cardType.getCoin(), nodes.size());

                Map<Integer, Temp> map = new HashMap<>(16);
                for (Node node : nodes) {
                    Temp temp = map.get(node.getGiftId());
                    if (Objects.isNull(temp)) {
                        temp = new Temp(1, node.getSend());
                        map.put(node.getGiftId(), temp);
                    } else {
                        temp.setNum(temp.getNum() + 1);
                    }
                }
                tapEggGift.putAll(map);
            } else {
//                logger.info("抽奖次数小于补偿礼物次数 取出对应前面的进行中奖");
                List<Node> nodes = new ArrayList<>();
                for (int i = 0; i < theCardReq.getNum(); i++) {
                    GiftCompensation compensation = giftCompensations.get(i);
                    nodes.add(new Gson().fromJson(compensation.getNodeJson(), Node.class));
                    compensation.setStatus(true);
                    compensation.setCompensationTime(new Date());
                    giftCompensationService.save(compensation);
                }
                handlePoolPersonData(cardTypeId, nodes, cardType.getCoin(), nodes.size());

                Map<Integer, Temp> map = new HashMap<>(16);
                for (Node node : nodes) {
                    Temp temp = map.get(node.getGiftId());
                    if (Objects.isNull(temp)) {
                        temp = new Temp(1, node.getSend());
                        map.put(node.getGiftId(), temp);
                    } else {
                        temp.setNum(temp.getNum() + 1);
                    }
                }
                tapEggGift = map;
            }
			-------------------------

        } else {
            // 获取随机砸蛋奖品
//            logger.info("当前用户无补偿礼物，开始到奖池抽奖");
            tagNum = theCardReq.getNum();
            if (!flagPersonPool) {
                tagNodeList.addAll(tagEgg(customer.getId(), cardTypeId, cardType.getCoin(), new BigDecimal(tagNum)));
            } else {
                tagNodeList.addAll(tagEggPerson2(customer.getId(), cardTypeId, cardType.getCoin(), new BigDecimal(tagNum)));
            }
        }

        // 如果礼物数小于抽奖次数 直接补上礼物ID为256的
        if (tagNum < tagNodeList.size()) {
            System.out.println("出现礼物数小于抽奖次数，补偿256礼物：" + customer.getId() + ":" + (tagNodeList.size() - tagNum));
//            for (int i = 0; i < (tagNodeList.size() - tagNum); i++) {
//                Node node = new Node();
//                node.setCoin(new BigDecimal(20));
//                node.setCoinInteger(20);
//                node.setGiftId(256);
//                node.setId(UUIDTool.getUUID());
//                node.setSend(false);
//                node.setThreshold(0);
//                tagNodeList.add(node);
//            }
        }

        for (Node node : tagNodeList) {
            Temp temp = tapEggGift.get(node.getGiftId());
            if (Objects.isNull(temp)) {
                temp = new Temp(1, node.getSend());
                tapEggGift.put(node.getGiftId(), temp);
            } else {
                temp.setNum(temp.getNum() + 1);
            }
        }

        // 中奖礼物放入背包；添加中奖记录
        List<Map<String, Object>> roomMessages = new ArrayList<>();
        List<Map<String, Object>> roomMessagesAll = new ArrayList<>();
        List<String> pushMessages = new ArrayList<>();
        List<Map<String, Object>> broadcastMessages = new ArrayList<>();
        BigDecimal cacheP = redisHelper.getIfNullSetDefault("EGG_" + cardTypeId + "_PERSON_" + ShiroUtils.getCustomerId() + "_P", new BigDecimal(0));
        if (flagPersonPool) {
            cacheP = new BigDecimal(7);
        }
        for (Map.Entry<Integer, Temp> entry : tapEggGift.entrySet()) {
            Gift gift = giftService.findById(entry.getKey());
            Temp temp = entry.getValue();

            // 中奖礼物放入用户背包
            customerKnapsackService.create(customer.getId(), gift.getId(), temp.getNum());
            // 添加砸蛋中奖记录
            theCardRecordService.create(customer.getId(), gift, theCardReq.getRoomId(), temp.getNum(), temp.isSend(), cardTypeId, cacheP.intValue(), theCardReq.getNum());

            // 自己聊天室砸蛋消息全发
            Map<String, Object> message = buildMessage(theCardReq.getRoomId(),
                    customer, gift.getImage().getUrl(), temp.getNum(), cardTypeId, gift.getCoin().toString(), gift.getName());
            roomMessages.add(message);

            // 砸蛋砸到大礼物发送聊天室消息
            if (gift.isBig()) {
                // 全服公屏
                roomMessagesAll.add(message);
            }

            // 砸蛋横幅通知消息
            if (temp.isSend()) {
                Map<String, Object> broadcastMessage = buildMessage(theCardReq.getRoomId(),
                        customer, gift.getImage().getUrl(), temp.getNum(), cardTypeId, gift.getCoin().toString(), gift.getName());
                broadcastMessage.put("show", true);
                broadcastMessage.put("price", gift.getCoin().toString());
                broadcastMessages.add(broadcastMessage);

                // 推送消息 厉害了！某某某刚刚砸出了“浪漫旅行”（52000）x1
                String tip1 = cardTypeId == 1 ? "魔力圈" : "念力圈";
                pushMessages.add("厉害了 " + customer.getCustomerInfo().getNickname()
                        + tip1 + "获得了" + gift.getName() + "x" + temp.getNum());

                SysMsg sysMsg = new SysMsg();
                sysMsg.setType(1);
                sysMsg.setContent("厉害了" + customer.getCustomerInfo().getNickname()
                        + tip1 + "获得了" + gift.getName() + "x" + temp.getNum());
                if (gift.getCoin().compareTo(new BigDecimal("9999")) >= 0) {
                    logger.info("设置信息");
                    sysMsg.setFlagShow(true);
                    sysMsg.setTip(customer.getCustomerInfo().getNickname() + "砸出" + gift.getName());
                }
                boolean fla = sysMsgService.create(sysMsg);
            }

            //构建返回值
            result.add(toRes(customer.getId(), gift, temp.getNum(), cardTypeId));
        }


        // 线上环境就推送消息 env.getActiveProfiles()[0]
        if ("prod".equals("prod")) {
            logger.info("推送消息");
            // 发送聊天室消息;全服消息;推送消息
            //messageHelper.sendTapEgg(customer.getId(), theCardReq.getRoomId(), roomMessages);
            //sendBroadcastMessage(customer.getId(), theCardReq.getRoomId(), broadcastMessages);


            //extra_egg_one 公屏消息(把中奖消息插入其他聊天房间的公屏消息)
            messageHelper.sendTapEggAll(PUBLIC_CHAT_ID, theCardReq.getRoomId(), roomMessagesAll);

            //extra_egg_all 客户端显示砸许愿神灯横幅通知 PUBLIC_CHAT_ID 让在自己房间也能看到横幅通知消息
            sendBroadcastMessage(PUBLIC_CHAT_ID, theCardReq.getRoomId(), broadcastMessages);

            // 发送是通知栏消息
            sendPushMessage(pushMessages);
            // 推送消息增加到系统消息里
        }
        return result;
    }

    private void handlePoolPersonData(Integer cardTypeId, List<Node> nodes, BigDecimal thisCoin, int size) {
        BigDecimal num = new BigDecimal(size);
        BigDecimal sum = nodes
                .stream()
                .map(Node::getCoin)
                .reduce(BigDecimal::add)
                .get();
        sum = sum.setScale(1, BigDecimal.ROUND_DOWN);

        String keyPre = "EGG_" + cardTypeId + "_PERSON_" + ShiroUtils.getCustomerId();

        BigDecimal cacheA = redisHelper.getIfNullSetDefault(keyPre + "_A", new BigDecimal(0));
        BigDecimal cacheB = redisHelper.getIfNullSetDefault(keyPre + "_B", new BigDecimal(0));
        BigDecimal cacheC = redisHelper.getIfNullSetDefault(keyPre + "_C", new BigDecimal(0));
        BigDecimal cacheD = redisHelper.getIfNullSetDefault(keyPre + "_D", new BigDecimal(0));
        BigDecimal cacheF = redisHelper.getIfNullSetDefault(keyPre + "_F", new BigDecimal(0));
        BigDecimal cacheY_G = redisHelper.getIfNullSetDefault(keyPre + "_Y_G", new BigDecimal(0));
        BigDecimal cacheY_C = redisHelper.getIfNullSetDefault(keyPre + "_Y_C", new BigDecimal(0));
        BigDecimal cacheZ_G = redisHelper.getIfNullSetDefault(keyPre + "_Z_G", new BigDecimal(0));
        BigDecimal cacheZ_C = redisHelper.getIfNullSetDefault(keyPre + "_Z_C", new BigDecimal(0));

        BigDecimal cacheP = redisHelper.getIfNullSetDefault(keyPre + "_P", new BigDecimal(0));
        // 循环奖池中轮数 默认是第0轮
        BigDecimal cacheL = redisHelper.getIfNullSetDefault(keyPre + "_L", new BigDecimal(0));

        if (cacheL.intValue() > 0) {
            if (cacheP.intValue() == 2) {
                // CY
                redisHelper.set(keyPre + "_C", cacheC.add(thisCoin.multiply(num)));
            } else if (cacheP.intValue() == 6) {
                // D
                redisHelper.set(keyPre + "_D", cacheD.add(thisCoin.multiply(num)));
            }
        } else {
            if (cacheP.intValue() == 0) {
                //FZ A
                redisHelper.set(keyPre + "_A", cacheA.add(thisCoin.multiply(num)));
                redisHelper.set(keyPre + "_F", cacheF.add(thisCoin.multiply(num)));
                redisHelper.set(keyPre + "_Z_C", cacheZ_C.add(thisCoin.multiply(num)));
                redisHelper.set(keyPre + "_Z_G", cacheZ_G.add(sum));
            } else if (cacheP.intValue() == 1) {
                // FZ B
                redisHelper.set(keyPre + "_B", cacheB.add(thisCoin.multiply(num)));
                redisHelper.set(keyPre + "_F", cacheF.add(thisCoin.multiply(num)));
                redisHelper.set(keyPre + "_Z_C", cacheZ_C.add(thisCoin.multiply(num)));
                redisHelper.set(keyPre + "_Z_G", cacheZ_G.add(sum));
            } else if (cacheP.intValue() == 2) {
                // FZ
                redisHelper.set(keyPre + "_F", cacheF.add(thisCoin.multiply(num)));
                redisHelper.set(keyPre + "_Z_C", cacheZ_C.add(thisCoin.multiply(num)));
                redisHelper.set(keyPre + "_Z_G", cacheZ_G.add(sum));
            } else if (cacheP.intValue() == 6) {
                // D
                redisHelper.set(keyPre + "_D", cacheD.add(thisCoin.multiply(num)));
            }
        }
        // Y算总爆率
        redisHelper.set(keyPre + "_Y_C", cacheY_C.add(thisCoin.multiply(num)));
        redisHelper.set(keyPre + "_Y_G", cacheY_G.add(sum));


    }

    /**
     * 获取许愿神灯礼物Map giftId <-> number  公共奖池
     *
     * @param giftNum  砸蛋次数
     * @param cardType 那种类型的许愿神灯
     * @return 获取砸蛋礼物Map
     */
    private Map<Integer, Temp> getTheCardGiftPool(Integer giftNum, String publicPoolKey, Integer cardType) {
        List<Node> nodes = new ArrayList<>();

        //需要从新生成的奖池里拿出的礼物数量
        int genNum = 0;

        //
        long size = redisHelper.sSize(publicPoolKey);
        logger.info("公共奖池中目前拥有的许愿神灯数量" + size);
        if (giftNum > size) {
            genNum = (int) (giftNum - size);
        }

        // 得到需要从旧奖池里取出来的礼物数量
        int oldNum = giftNum - genNum;
        logger.info("用户需要抽取的礼物数量" + giftNum);
        logger.info("需要从当前奖池中拿出来的礼物数量" + oldNum);
        logger.info("需要从新奖池中拿出来的礼物数量" + genNum);


        // 获取随机砸蛋奖品
        if (genNum > 0) {
            long configPoolSize = getConfigPoolSize(cardType, true);
            if (configPoolSize == 0) {
                if (cardType == 1)
                    ExceptionUtils.throwResponseException(MarkCode.build("当前许愿神灯类的公共奖池尚未配置,请等管理员配置后再使用"));
                else ExceptionUtils.throwResponseException(MarkCode.build("当前魔法神灯类的公共奖池尚未配置,请等管理员配置后再使用"));
            }
            logger.info("当前公共奖池配置每次可生成的许愿神灯数量" + configPoolSize);

            if (oldNum > 0) {
                logger.info("从当前奖池中取礼物的数量" + oldNum);
                nodes.addAll(redisHelper.sPop(publicPoolKey, oldNum, Node.class));
            }

            if (genNum < configPoolSize) {
                logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
                toGenCardPoolAndGetGift(publicPoolKey, genNum, (List<Node>) nodes, cardType, true);
            } else {
                long num = genNum / configPoolSize;
                logger.info("需要重复生成奖池的次数" + num);
                long value = genNum % configPoolSize;
                logger.info("求余" + value);
                logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
                if (value == 0) {
                    for (int i = 0; i < num; i++) {
                        toGenCardPoolAndGetGift(publicPoolKey, ((int) configPoolSize), (List<Node>) nodes, cardType, true);
                    }
                } else {
                    logger.info("需要重复生成奖池的次数" + num + 1);
                    for (int i = 0; i < num + 1; i++) {
                        if (i == num) {
                            toGenCardPoolAndGetGift(publicPoolKey, ((int) value), (List<Node>) nodes, cardType, true);
                        } else {
                            toGenCardPoolAndGetGift(publicPoolKey, ((int) configPoolSize), (List<Node>) nodes, cardType, true);
                        }

                    }
                }

            }
        } else {
            logger.info("当前公共奖池的许愿神灯数量充足，无需重新生成");
            nodes.addAll(redisHelper.sPop(publicPoolKey, oldNum, Node.class));
        }

        logger.info("砸蛋结束：" + "nodes" + nodes.size() + "giftNum" + giftNum);
        Assert.isTrue(MarkCode.build("本期奖池礼物少于您所砸次数，请稍后"), nodes.size() == giftNum);

        Map<Integer, Temp> map = new HashMap<>(16);
        for (Node node : nodes) {
            Temp temp = map.get(node.getGiftId());
            if (Objects.isNull(temp)) {
                temp = new Temp(1, node.getSend());
                map.put(node.getGiftId(), temp);
            } else {
                temp.setNum(temp.getNum() + 1);
            }
        }
        return map;
    }

    /**
     * 一次个人奖池2砸蛋
     * POOL_cardType_poolType_PERSON_customerId
     * POOL_cardType_poolType_PUBLIC
     *
     * @return
     */
    private List<Node> tagEggPerson2(Integer customerId, Integer cardType, BigDecimal thisCoin, BigDecimal num) {
        // 个人此轮奖池累计消费金币
        List<Node> nodeList = new ArrayList<>();
        String keyPre = "POOL_" + cardType + "_7_PERSON" + customerId;
        String customerTotalCoinKey = keyPre + "_TOTAL_COIN";
        BigDecimal customerTotalCoin = redisHelper.get(customerTotalCoinKey, BigDecimal.class);
        if (customerTotalCoin == null) {
            customerTotalCoin = new BigDecimal(0);
            redisHelper.set(customerTotalCoinKey, customerTotalCoin);
        }

        // 从缓存中拿最小的必出礼物 判断是否大于
        String thresholdKey = keyPre + "_THRESHOLD";
        // 缓存中从小到大排序的数据
        List<Node> thresholdList = redisHelper.getCollection(thresholdKey, List.class, Node.class);
        if (thresholdList == null) {
            thresholdList = new ArrayList<>();
        }
        Integer addCoin = customerTotalCoin.add(thisCoin.multiply(num)).intValue();
        int thresholdCount = 0;

        Iterator<Node> sListIterator = thresholdList.iterator();
        while (sListIterator.hasNext()) {
            Node e = sListIterator.next();
            if (e.getThreshold() < addCoin && thresholdCount <= num.intValue()) {
                sListIterator.remove();
                thresholdCount++;
            }
        }
        // 更新缓存
        redisHelper.set(thresholdKey, thresholdList);

        // 用于计算此轮该添加多少累计金额
        int thisP = thresholdCount;

        // 剩余抽奖次数
        int laveNum = num.intValue() - thresholdCount;
        if (laveNum > 0) {
            //需要从新生成的奖池里拿出的礼物数量
            int genNum = 0;

            String keyNode = keyPre + "_GIFT";
            // 去个人奖池抽奖  如果当前个人奖池的数量小于用户需要翻牌的数量，就重新生成Pool
            long size = redisHelper.sSize(keyNode);
            if (laveNum > size) {
                genNum = (int) (laveNum - size);
            }

            // 得到需要从旧奖池里取出来的礼物数量
            int oldNum = laveNum - genNum;
//            logger.info("用户需要抽取的礼物数量" + laveNum);
//            logger.info("需要从当前奖池中拿出来的礼物数量" + oldNum);
//            logger.info("需要从新奖池中拿出来的礼物数量" + genNum);

            boolean createPool = false;
            if (genNum > 0) {
                // 获取当前配置的奖池需要生成的次数
                long configPoolSize = getConfigPoolSizeNew(cardType, 7);
                if (configPoolSize == 0) {
                    ExceptionUtils.throwResponseException(MarkCode.build("当前奖池尚未配置,请等管理员配置后再使用"));
                }

                if (oldNum > 0) {
//                    logger.info("从当前奖池中取礼物的数量" + oldNum);
                    nodeList.addAll(redisHelper.sPop(keyNode, oldNum, Node.class));
                }

                if (genNum < configPoolSize) {
//                    logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
                    // 生成奖池
                    createPool = true;
                    generateTapEggPoolNew(keyNode, 7, cardType);
                    nodeList.addAll(redisHelper.sPop(keyNode, genNum, Node.class));
                    thisP = genNum;

                } else {
                    long poolNum = genNum / configPoolSize;
//                    logger.info("需要重复生成奖池的次数" + poolNum);
                    long value = genNum % configPoolSize;
//                    logger.info("求余" + value);
//                    logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
                    if (value == 0) {
                        for (int i = 0; i < poolNum; i++) {
                            // 生成奖池
                            createPool = true;
                            generateTapEggPoolNew(keyNode, 7, cardType);
                            nodeList.addAll(redisHelper.sPop(keyNode, ((int) configPoolSize), Node.class));
                            thisP = ((int) configPoolSize);
                        }
                    } else {
//                        logger.info("需要重复生成奖池的次数" + (poolNum + 1));
                        for (int i = 0; i < poolNum + 1; i++) {
                            if (i == poolNum) {
                                createPool = true;
                                generateTapEggPoolNew(keyNode, 7, cardType);
                                nodeList.addAll(redisHelper.sPop(keyNode, ((int) value), Node.class));
                                thisP = ((int) value);
                            } else {
                                createPool = true;
                                generateTapEggPoolNew(keyNode, 7, cardType);
                                nodeList.addAll(redisHelper.sPop(keyNode, ((int) configPoolSize), Node.class));
                                thisP = ((int) configPoolSize);
                            }
                        }
                    }
                }
                // 记录此轮用户累计消费金币++ 不需要累计原来的
                redisHelper.set(customerTotalCoinKey, thisCoin.multiply(new BigDecimal(thisP)));
            } else {
//                logger.info("当前个人奖池的许愿神灯数量充足，无需重新生成");
                nodeList.addAll(redisHelper.sPop(keyNode, oldNum, Node.class));
                thisP = thisP + oldNum;
                // 记录此轮用户累计消费金币++
                redisHelper.set(customerTotalCoinKey, customerTotalCoin.add(thisCoin.multiply(new BigDecimal(thisP))));
            }
            if (createPool) {
                // 初始化必现奖池
                generateTapEggPoolNewForThreshold(thresholdKey, 7, cardType);
            }

        }

        return nodeList;
    }

    /**
     * 一次复杂奖池砸蛋
     * EGG_cardType_PERSON_customerId_xxx
     * <p>
     * POOL_cardType_poolType_PERSON_customerId
     * POOL_cardType_poolType_PUBLIC
     *
     * @return
     */
    private List<Node> tagEgg(Integer customerId, Integer cardType, BigDecimal thisCoin, BigDecimal num) {
        // 根据缓存数据和配置数据，判断此次抽奖去哪个奖池 CONFIG_A
        BigDecimal configA = redisHelper.getIfNullSetDefault("CONFIG_A", new BigDecimal(5000));
        BigDecimal configB = redisHelper.getIfNullSetDefault("CONFIG_B", new BigDecimal(10000));
        BigDecimal configC = redisHelper.getIfNullSetDefault("CONFIG_C", new BigDecimal(5000));
        BigDecimal configD = redisHelper.getIfNullSetDefault("CONFIG_D", new BigDecimal(10000));
        BigDecimal configF = redisHelper.getIfNullSetDefault("CONFIG_F", new BigDecimal(20000));
        BigDecimal configX = redisHelper.getIfNullSetDefault("CONFIG_X", new BigDecimal(20));
        BigDecimal configY = redisHelper.getIfNullSetDefault("CONFIG_Y", new BigDecimal(1.4d));
        BigDecimal configZ = redisHelper.getIfNullSetDefault("CONFIG_Z", new BigDecimal(1.4d));
        if (cardType.intValue() == 2) {
            configA = redisHelper.getIfNullSetDefault("CONFIG_A_2", new BigDecimal(5000));
            configB = redisHelper.getIfNullSetDefault("CONFIG_B_2", new BigDecimal(5000));
            configC = redisHelper.getIfNullSetDefault("CONFIG_C_2", new BigDecimal(15000));
            configD = redisHelper.getIfNullSetDefault("CONFIG_D_2", new BigDecimal(10000));
            configF = redisHelper.getIfNullSetDefault("CONFIG_F_2", new BigDecimal(20000));
            configX = redisHelper.getIfNullSetDefault("CONFIG_X_2", new BigDecimal(50));
            configY = redisHelper.getIfNullSetDefault("CONFIG_Y_2", new BigDecimal(1.4d));
            configZ = redisHelper.getIfNullSetDefault("CONFIG_Z_2", new BigDecimal(1.4d));
        }
        return tagEgg(customerId, cardType, thisCoin, num, configA, configB, configC, configD, configF, configX, configY, configZ);
    }

    private List<Node> tagEgg(Integer customerId, Integer cardType, BigDecimal thisCoin, BigDecimal num,
                              BigDecimal configA, BigDecimal configB, BigDecimal configC, BigDecimal configD, BigDecimal configF,
                              BigDecimal configX, BigDecimal configY, BigDecimal configZ) {
        // 个人数据缓存
        String keyPre = "EGG_" + cardType + "_PERSON_" + customerId;

        BigDecimal cacheA = redisHelper.getIfNullSetDefault(keyPre + "_A", new BigDecimal(0));
        BigDecimal cacheB = redisHelper.getIfNullSetDefault(keyPre + "_B", new BigDecimal(0));
        BigDecimal cacheC = redisHelper.getIfNullSetDefault(keyPre + "_C", new BigDecimal(0));
        BigDecimal cacheD = redisHelper.getIfNullSetDefault(keyPre + "_D", new BigDecimal(0));
        BigDecimal cacheF = redisHelper.getIfNullSetDefault(keyPre + "_F", new BigDecimal(0));
        // Y的礼物价值
        BigDecimal cacheY_G = redisHelper.getIfNullSetDefault(keyPre + "_Y_G", new BigDecimal(0));
        // Y的金币花费
        BigDecimal cacheY_C = redisHelper.getIfNullSetDefault(keyPre + "_Y_C", new BigDecimal(0));
        BigDecimal cacheZ_G = redisHelper.getIfNullSetDefault(keyPre + "_Z_G", new BigDecimal(0));
        BigDecimal cacheZ_C = redisHelper.getIfNullSetDefault(keyPre + "_Z_C", new BigDecimal(0));

        // 实际爆率，礼物价值/抽奖金币
        // 上次是在哪个奖池 0试水 1公共奖池1 2公共奖池2 6个人奖池1
        BigDecimal cacheP = redisHelper.getIfNullSetDefault(keyPre + "_P", new BigDecimal(0));
        // 循环奖池中轮数 默认是第0轮
        BigDecimal cacheL = redisHelper.getIfNullSetDefault(keyPre + "_L", new BigDecimal(0));
        // 分别调用封装的不同奖池抽奖 公共奖池key不能带customerId 抽奖完毕后更新相关缓存数据
        List<Node> nodeList;
        if (cacheL.intValue() > 0) {
            if (cacheP.intValue() == 2) {
                if (cacheC.intValue() > configC.intValue() && cacheY_G.divide(cacheY_C, 2, BigDecimal.ROUND_HALF_UP).doubleValue() < configY.doubleValue()) {
                    // 去个人奖池1 cacheP改成6
                    // 重置个人奖池1
                    redisHelper.del("POOL_" + cardType + "_6_PERSON" + customerId + "_THRESHOLD");
                    redisHelper.del("POOL_" + cardType + "_6_PERSON" + customerId + "_GIFT");
                    // 新进入个池1 重置累计消费金额
                    cacheD = new BigDecimal(0);
                    nodeList = getTheCardGift6(customerId, cardType, thisCoin, keyPre, cacheD, num);
                    redisHelper.set(keyPre + "_P", new BigDecimal(6));
                } else {
                    // 还在公共奖池2
                    nodeList = getTheCardGift2("POOL_" + cardType + "_2_PUBLIC", cardType, cacheL, keyPre, cacheC, cacheF, cacheZ_C, cacheZ_G, thisCoin, num.intValue());
                }
            } else {
                if (cacheD.intValue() > configD.intValue()) {
                    // 去公共奖池2 cacheP改成2
                    // 新进入公池2初始化此轮金额
                    cacheC = new BigDecimal(0);
                    nodeList = getTheCardGift2("POOL_" + cardType + "_2_PUBLIC", cardType, cacheL, keyPre, cacheC, cacheF, cacheZ_C, cacheZ_G, thisCoin, num.intValue());
                    redisHelper.set(keyPre + "_P", new BigDecimal(2));

                } else {
                    // 还在个人奖池1
                    nodeList = getTheCardGift6(customerId, cardType, thisCoin, keyPre, cacheD, num);
                }
            }
        } else {
            if (cacheP.intValue() == 6) {
                if (cacheD.intValue() > configD.intValue()) {
                    // 去公共奖池2 cacheL改为1 cacheP改为2
                    nodeList = getTheCardGift2("POOL_" + cardType + "_2_PUBLIC", cardType, cacheL, keyPre, cacheC, cacheF, cacheZ_C, cacheZ_G, thisCoin, num.intValue());
                    redisHelper.set(keyPre + "_P", new BigDecimal(2));
                    redisHelper.set(keyPre + "_L", new BigDecimal(1));
                } else {
                    // 继续在个人奖池1
                    nodeList = getTheCardGift6(customerId, cardType, thisCoin, keyPre, cacheD, num);
                }
            } else if (cacheP.intValue() == 2) {
                if (cacheF.intValue() > configF.intValue() && cacheZ_G.divide(cacheZ_C, 2, BigDecimal.ROUND_HALF_UP).doubleValue() < configZ.doubleValue()) {
                    // 去个人奖池1 cacheP改为6
                    // 重置个人奖池1
                    redisHelper.del("POOL_" + cardType + "_6_PERSON" + customerId + "_THRESHOLD");
                    redisHelper.del("POOL_" + cardType + "_6_PERSON" + customerId + "_GIFT");
                    // 新进入个池1 重置累计消费金额
                    cacheD = new BigDecimal(0);
                    nodeList = getTheCardGift6(customerId, cardType, thisCoin, keyPre, cacheD, num);
                    redisHelper.set(keyPre + "_P", new BigDecimal(6));
                } else {
                    // 继续在公共奖池2
                    nodeList = getTheCardGift2("POOL_" + cardType + "_2_PUBLIC", cardType, cacheL, keyPre, cacheC, cacheF, cacheZ_C, cacheZ_G, thisCoin, num.intValue());
                }
            } else if (cacheP.intValue() == 1) {
                if (cacheB.intValue() > configB.intValue()) {
                    // 去个人奖池2 cacheP改为2
                    nodeList = getTheCardGift2("POOL_" + cardType + "_2_PUBLIC", cardType, cacheL, keyPre, cacheC, cacheF, cacheZ_C, cacheZ_G, thisCoin, num.intValue());
                    redisHelper.set(keyPre + "_P", new BigDecimal(2));
                } else {
                    // 继续在公共奖池1
                    nodeList = getTheCardGift1("POOL_" + cardType + "_1_PUBLIC", cardType, num.intValue());
                    // 修改 B F Z
                    redisHelper.set(keyPre + "_B", cacheB.add(thisCoin.multiply(num)));
                    redisHelper.set(keyPre + "_F", cacheF.add(thisCoin.multiply(num)));
                    redisHelper.set(keyPre + "_Z_C", cacheZ_C.add(thisCoin.multiply(num)));
                    BigDecimal sum = nodeList
                            .stream()
                            .map(Node::getCoin)
                            .reduce(BigDecimal::add)
                            .get();
                    sum = sum.setScale(1, BigDecimal.ROUND_DOWN);
                    redisHelper.set(keyPre + "_Z_G", cacheZ_G.add(sum));
                }
            } else {
                if (cacheA.intValue() > configA.intValue()) {
                    // 去公共奖池1 cacheP改为1
                    nodeList = getTheCardGift1("POOL_" + cardType + "_1_PUBLIC", cardType, num.intValue());
                    // 修改 B F Z
                    redisHelper.set(keyPre + "_B", cacheB.add(thisCoin.multiply(num)));
                    redisHelper.set(keyPre + "_P", new BigDecimal(1));

                } else {
                    // 留在试水奖池
                    nodeList = getTheCardGift0("POOL_" + cardType + "_0_PUBLIC", configX, customerId, cardType, num.intValue());
                    // 修改 F Z A
                    redisHelper.set(keyPre + "_A", cacheA.add(thisCoin.multiply(num)));

                }
                redisHelper.set(keyPre + "_F", cacheF.add(thisCoin.multiply(num)));
                redisHelper.set(keyPre + "_Z_C", cacheZ_C.add(thisCoin.multiply(num)));
                BigDecimal sum = nodeList
                        .stream()
                        .map(Node::getCoin)
                        .reduce(BigDecimal::add)
                        .get();
                sum = sum.setScale(1, BigDecimal.ROUND_DOWN);
                redisHelper.set(keyPre + "_Z_G", cacheZ_G.add(sum));
            }
        }
        // 总消费 总礼物价值++
        redisHelper.set(keyPre + "_Y_C", cacheY_C.add(thisCoin.multiply(num)));
        BigDecimal sumYg = nodeList
                .stream()
                .map(Node::getCoin)
                .reduce(BigDecimal::add)
                .get();
        redisHelper.set(keyPre + "_Y_G", cacheY_G.add(sumYg));
        return nodeList;
    }

    private boolean goodLuck(Integer customerId) {
        int a = (customerId + "").hashCode() % 100;
        if (a < 0) {
            a = 0 - a;
        }
        return a < 70 ? true : false;
    }

    /**
     * 试水奖池 公共奖池
     * 尽量保证70%的用户爆率>configX
     * 判断礼物价值 把小礼物或者大礼物放进去 再抽一次
     *
     * @return
     */
    private List<Node> getTheCardGift0(String poolKey, BigDecimal configX, Integer customerId, Integer cardType, Integer laveNum) {
        List<Node> nodeList = new ArrayList<>();
        //需要从新生成的奖池里拿出的礼物数量
        int genNum = 0;
        long size = redisHelper.sSize(poolKey);
        if (laveNum > size) {
            genNum = (int) (laveNum - size);
        }

        // 得到需要从旧奖池里取出来的礼物数量
        int oldNum = laveNum - genNum;
//        logger.info("用户需要抽取的礼物数量" + laveNum);
//        logger.info("需要从当前奖池中拿出来的礼物数量" + oldNum);
//        logger.info("需要从新奖池中拿出来的礼物数量" + genNum);

        if (genNum > 0) {
            pool0(cardType, oldNum, nodeList, poolKey, genNum);
        } else {
            nodeList.addAll(redisHelper.sPop(poolKey, oldNum, Node.class));
        }
        boolean flag = false;
        int nodeSub = 0;
        if (nodeList.size() == 1) {
            nodeSub = 0;
            if (goodLuck(customerId) && nodeList.get(0).getCoin().intValue() < configX.intValue()) {
                flag = true;
            } else if (!goodLuck(customerId) && nodeList.get(0).getCoin().intValue() > configX.intValue()) {
                flag = true;
            }
        } else {
            flag = true;
            BigDecimal temp = nodeList.get(0).getCoin();
            if (goodLuck(customerId)) {
                // 找最小的
                for (int i = 0; i < nodeList.size(); i++) {
                    if (nodeList.get(i).getCoin().intValue() < temp.intValue()) {
                        temp = nodeList.get(i).getCoin();
                        nodeSub = i;
                    }
                }
            } else {
                // 找最大的
                for (int i = 0; i < nodeList.size(); i++) {
                    if (nodeList.get(i).getCoin().intValue() > temp.intValue()) {
                        temp = nodeList.get(i).getCoin();
                        nodeSub = i;
                    }
                }
            }
        }
        if (flag && laveNum.intValue() == 100) {
            // 取出 放入奖池 重新抽一次
            redisHelper.sSet(poolKey, nodeList.get(nodeSub));
            nodeList.remove(nodeSub);
            nodeList.add(redisHelper.sPop(poolKey, Node.class));
        }

        return nodeList;
    }

    /**
     * 公共奖池1
     *
     * @return
     */
    private List<Node> getTheCardGift1(String poolKey, Integer cardType, Integer laveNum) {
        List<Node> nodeList = new ArrayList<>();
        //需要从新生成的奖池里拿出的礼物数量
        int genNum = 0;
        long size = redisHelper.sSize(poolKey);
        if (laveNum > size) {
            genNum = (int) (laveNum - size);
        }

        // 得到需要从旧奖池里取出来的礼物数量
        int oldNum = laveNum - genNum;
//        logger.info("用户需要抽取的礼物数量" + laveNum);
//        logger.info("需要从当前奖池中拿出来的礼物数量" + oldNum);
//        logger.info("需要从新奖池中拿出来的礼物数量" + genNum);

        if (genNum > 0) {
            // 获取当前配置的奖池需要生成的次数
            pool1(cardType, oldNum, nodeList, poolKey, genNum);
        } else {
            logger.info("当前个人奖池的许愿神灯数量充足，无需重新生成");
            nodeList.addAll(redisHelper.sPop(poolKey, oldNum, Node.class));
        }
        return nodeList;

    }


    /**
     * 公共奖池2
     * 注意如果cacheL>0 新生成的奖池 需要重新累计cacheY两个数值 cacheC
     *
     * @return
     */
    private List<Node> getTheCardGift2(String poolKey, Integer cardType, BigDecimal cacheL, String keyPre, BigDecimal cacheC,
                                       BigDecimal cacheF, BigDecimal cacheZ_C, BigDecimal cacheZ_G, BigDecimal thisCoin, Integer laveNum) {
        List<Node> nodeList = new ArrayList<>();
        //需要从新生成的奖池里拿出的礼物数量
        int genNum = 0;
        long size = redisHelper.sSize(poolKey);
        if (laveNum > size) {
            genNum = (int) (laveNum - size);
        }

        // 得到需要从旧奖池里取出来的礼物数量
        int oldNum = laveNum - genNum;
//        logger.info("用户需要抽取的礼物数量" + laveNum);
//        logger.info("需要从当前奖池中拿出来的礼物数量" + oldNum);
//        logger.info("需要从新奖池中拿出来的礼物数量" + genNum);

        int thisP = 0;
        boolean createPool = false;
        if (genNum > 0) {
            // 获取当前配置的奖池需要生成的次数
            pool2(cardType, oldNum, nodeList, poolKey, genNum, createPool, thisP);
        } else {
//            logger.info("当前个人奖池的许愿神灯数量充足，无需重新生成");
            nodeList.addAll(redisHelper.sPop(poolKey, oldNum, Node.class));
            thisP = thisP + oldNum;
        }

        BigDecimal sum = nodeList
                .stream()
                .map(Node::getCoin)
                .reduce(BigDecimal::add)
                .get();
        sum = sum.setScale(1, BigDecimal.ROUND_DOWN);

        if (cacheL.intValue() > 0) {
            // 第一轮开始后的新奖池
            redisHelper.set(keyPre + "_C", cacheC.add(thisCoin.multiply(new BigDecimal(thisP))));
//            redisHelper.set(keyPre + "_Y_C", thisCoin.multiply(new BigDecimal(thisP)));
//            redisHelper.set(keyPre + "_Y_G", sum);
        } else {
            // 没开始第一轮循环 不管是不是新奖池 加F Z
            redisHelper.set(keyPre + "_F", cacheF.add(thisCoin.multiply(new BigDecimal(thisP))));
            redisHelper.set(keyPre + "_Z_C", cacheZ_C.add(thisCoin.multiply(new BigDecimal(thisP))));
            redisHelper.set(keyPre + "_Z_G", cacheZ_G.add(sum));
        }
//        if (nodeList.size() < laveNum.intValue()) {
//            getTheCardGift2(poolKey, cardType, cacheL, keyPre, cacheC, cacheF, cacheZ_C, cacheZ_G, thisCoin, laveNum - nodeList.size());
//        }
        return nodeList;
    }

    private void pool0(Integer cardType, int oldNum, List<Node> nodeList, String poolKey, int genNum) {
        // 获取当前配置的奖池需要生成的次数
        long configPoolSize = getConfigPoolSizeNew(cardType, 0);
        if (configPoolSize == 0) {
            ExceptionUtils.throwResponseException(MarkCode.build("当前奖池尚未配置,请等管理员配置后再使用"));
        }

        if (oldNum > 0) {
//                logger.info("从当前奖池中取礼物的数量" + oldNum);
            nodeList.addAll(redisHelper.sPop(poolKey, oldNum, Node.class));
        }

        if (genNum < configPoolSize) {
//                logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
            // 生成奖池
            generateTapEggPoolNew(poolKey, 0, cardType);
            nodeList.addAll(redisHelper.sPop(poolKey, genNum, Node.class));

        } else {
            long poolNum = genNum / configPoolSize;
//                logger.info("需要重复生成奖池的次数" + poolNum);
            long value = genNum % configPoolSize;
//                logger.info("求余" + value);
//                logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
            if (value == 0) {
                for (int i = 0; i < poolNum; i++) {
                    // 生成奖池
                    generateTapEggPoolNew(poolKey, 0, cardType);
                    nodeList.addAll(redisHelper.sPop(poolKey, ((int) configPoolSize), Node.class));
                }
            } else {
//                    logger.info("需要重复生成奖池的次数" + (poolNum + 1));
                for (int i = 0; i < poolNum + 1; i++) {
                    if (i == poolNum) {
                        generateTapEggPoolNew(poolKey, 0, cardType);
                        nodeList.addAll(redisHelper.sPop(poolKey, ((int) value), Node.class));
                    } else {
                        generateTapEggPoolNew(poolKey, 0, cardType);
                        nodeList.addAll(redisHelper.sPop(poolKey, ((int) configPoolSize), Node.class));
                    }
                }

            }
        }
    }

    private void pool1(Integer cardType, int oldNum, List<Node> nodeList, String poolKey, int genNum) {
        long configPoolSize = getConfigPoolSizeNew(cardType, 1);
        if (configPoolSize == 0) {
            ExceptionUtils.throwResponseException(MarkCode.build("当前奖池尚未配置,请等管理员配置后再使用"));
        }

        if (oldNum > 0) {
//                logger.info("从当前奖池中取礼物的数量" + oldNum);
            nodeList.addAll(redisHelper.sPop(poolKey, oldNum, Node.class));
        }

        if (genNum < configPoolSize) {
//                logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
            // 生成奖池
            generateTapEggPoolNew(poolKey, 1, cardType);
            nodeList.addAll(redisHelper.sPop(poolKey, genNum, Node.class));

        } else {
            long poolNum = genNum / configPoolSize;
//                logger.info("需要重复生成奖池的次数" + poolNum);
            long value = genNum % configPoolSize;
//                logger.info("求余" + value);
//                logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
            if (value == 0) {
                for (int i = 0; i < poolNum; i++) {
                    // 生成奖池
                    generateTapEggPoolNew(poolKey, 1, cardType);
                    nodeList.addAll(redisHelper.sPop(poolKey, ((int) configPoolSize), Node.class));
                }
            } else {
                logger.info("需要重复生成奖池的次数" + (poolNum + 1));
                for (int i = 0; i < poolNum + 1; i++) {
                    if (i == poolNum) {
                        generateTapEggPoolNew(poolKey, 1, cardType);
                        nodeList.addAll(redisHelper.sPop(poolKey, ((int) value), Node.class));
                    } else {
                        generateTapEggPoolNew(poolKey, 1, cardType);
                        nodeList.addAll(redisHelper.sPop(poolKey, ((int) configPoolSize), Node.class));
                    }
                }

            }
        }
    }

    private void pool2(Integer cardType, int oldNum, List<Node> nodeList, String poolKey, int genNum, boolean createPool, int thisP) {
        long configPoolSize = getConfigPoolSizeNew(cardType, 2);
        if (configPoolSize == 0) {
            ExceptionUtils.throwResponseException(MarkCode.build("当前奖池尚未配置,请等管理员配置后再使用"));
        }

        if (oldNum > 0) {
            logger.info("从当前奖池中取礼物的数量" + oldNum);
            nodeList.addAll(redisHelper.sPop(poolKey, oldNum, Node.class));
        }

        if (genNum < configPoolSize) {
            logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
            // 生成奖池
            createPool = true;
            generateTapEggPoolNew(poolKey, 2, cardType);
            nodeList.addAll(redisHelper.sPop(poolKey, genNum, Node.class));
            thisP = genNum;

        } else {
            long poolNum = genNum / configPoolSize;
//                logger.info("需要重复生成奖池的次数" + poolNum);
            long value = genNum % configPoolSize;
//                logger.info("求余" + value);
//                logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
            if (value == 0) {
                for (int i = 0; i < poolNum; i++) {
                    // 生成奖池
                    createPool = true;
                    generateTapEggPoolNew(poolKey, 2, cardType);
                    nodeList.addAll(redisHelper.sPop(poolKey, ((int) configPoolSize), Node.class));
                    thisP = ((int) configPoolSize);
                }
            } else {
//                    logger.info("需要重复生成奖池的次数" + (poolNum + 1));
                for (int i = 0; i < poolNum + 1; i++) {
                    if (i == poolNum) {
                        createPool = true;
                        generateTapEggPoolNew(poolKey, 2, cardType);
                        nodeList.addAll(redisHelper.sPop(poolKey, ((int) value), Node.class));
                        thisP = ((int) value);
                    } else {
                        createPool = true;
                        generateTapEggPoolNew(poolKey, 2, cardType);
                        nodeList.addAll(redisHelper.sPop(poolKey, ((int) configPoolSize), Node.class));
                        thisP = ((int) configPoolSize);
                    }
                }

            }
        }
    }

    private List<Node> getTheCardGift6(Integer customerId, Integer cardType, BigDecimal thisCoin, String keyPreParam, BigDecimal cacheD, BigDecimal num) {
        List<Node> nodeList = new ArrayList<>();
        String keyPre = "POOL_" + cardType + "_6_PERSON" + customerId;

        // 从缓存中拿最小的必出礼物 判断是否大于
        String thresholdKey = keyPre + "_THRESHOLD";
        // 缓存中从小到大排序的数据
        List<Node> thresholdList = redisHelper.getCollection(thresholdKey, List.class, Node.class);

        Integer addCoin = cacheD.add(thisCoin.multiply(num)).intValue();
        int thresholdCount = 0;

        if (null == thresholdList) {
            generateTapEggPoolNewForThreshold(thresholdKey, 6, cardType);
            thresholdList = redisHelper.getCollection(thresholdKey, List.class, Node.class);
            if (null == thresholdList) {
                thresholdList = new ArrayList<>();
            }
        }
        Iterator<Node> sListIterator = thresholdList.iterator();
        while (sListIterator.hasNext()) {
            Node e = sListIterator.next();
            if (e.getThreshold() < addCoin && thresholdCount <= num.intValue()) {
                sListIterator.remove();
                thresholdCount++;
            }
        }
        // 更新缓存
        redisHelper.set(thresholdKey, thresholdList);

        // 用于计算此轮该添加多少累计金额
        int thisP = thresholdCount;

        // 剩余抽奖次数
        int laveNum = num.intValue() - thresholdCount;
        if (laveNum > 0) {
            //需要从新生成的奖池里拿出的礼物数量
            int genNum = 0;

            String keyNode = keyPre + "_GIFT";
            // 去个人奖池抽奖  如果当前个人奖池的数量小于用户需要翻牌的数量，就重新生成Pool
            long size = redisHelper.sSize(keyNode);
            if (laveNum > size) {
                genNum = (int) (laveNum - size);
            }

            // 得到需要从旧奖池里取出来的礼物数量
            int oldNum = laveNum - genNum;
//            logger.info("用户需要抽取的礼物数量" + laveNum);
//            logger.info("需要从当前奖池中拿出来的礼物数量" + oldNum);
//            logger.info("需要从新奖池中拿出来的礼物数量" + genNum);

            boolean createPool = false;
            if (genNum > 0) {
                // 获取当前配置的奖池需要生成的次数
                long configPoolSize = getConfigPoolSizeNew(cardType, 6);
                if (configPoolSize == 0) {
                    ExceptionUtils.throwResponseException(MarkCode.build("当前奖池尚未配置,请等管理员配置后再使用"));
                }

                if (oldNum > 0) {
//                    logger.info("从当前奖池中取礼物的数量" + oldNum);
                    nodeList.addAll(redisHelper.sPop(keyNode, oldNum, Node.class));
                }

                if (genNum < configPoolSize) {
//                    logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
                    // 生成奖池
                    createPool = true;
                    generateTapEggPoolNew(keyNode, 6, cardType);
                    nodeList.addAll(redisHelper.sPop(keyNode, genNum, Node.class));
                    thisP = genNum;

                } else {
                    long poolNum = genNum / configPoolSize;
//                    logger.info("需要重复生成奖池的次数" + poolNum);
                    long value = genNum % configPoolSize;
//                    logger.info("求余" + value);
//                    logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
                    if (value == 0) {
                        for (int i = 0; i < poolNum; i++) {
                            // 生成奖池
                            createPool = true;
                            generateTapEggPoolNew(keyNode, 6, cardType);
                            nodeList.addAll(redisHelper.sPop(keyNode, ((int) configPoolSize), Node.class));
                            thisP = ((int) configPoolSize);
                        }
                    } else {
//                        logger.info("需要重复生成奖池的次数" + (poolNum + 1));
                        for (int i = 0; i < poolNum + 1; i++) {
                            if (i == poolNum) {
                                createPool = true;
                                generateTapEggPoolNew(keyNode, 6, cardType);
                                nodeList.addAll(redisHelper.sPop(keyNode, ((int) value), Node.class));
                                thisP = ((int) value);
                            } else {
                                createPool = true;
                                generateTapEggPoolNew(keyNode, 6, cardType);
                                nodeList.addAll(redisHelper.sPop(keyNode, ((int) configPoolSize), Node.class));
                                thisP = ((int) configPoolSize);
                            }
                        }

                    }
                }
//                // 记录此轮用户累计消费金币++  重新生成奖池 不加原来的数据
//                redisHelper.set(keyPreParam + "_D", thisCoin.multiply(new BigDecimal(thisP)));
            } else {
//                logger.info("当前个人奖池的许愿神灯数量充足，无需重新生成");
                nodeList.addAll(redisHelper.sPop(keyNode, oldNum, Node.class));
//                thisP = thisP + oldNum;
//                // 记录此轮用户累计消费金币++
//                redisHelper.set(keyPreParam + "_D", cacheD.add(thisCoin.multiply(new BigDecimal(thisP))));
            }
            if (createPool) {
                // 初始化必现奖池
                generateTapEggPoolNewForThreshold(thresholdKey, 6, cardType);
            }

        }
        redisHelper.set(keyPreParam + "_D", cacheD.add(thisCoin.multiply(num)));

        return nodeList;
    }

    /**
     * 获取许愿神灯礼物Map giftId <-> number
     *
     * @param giftNum         砸蛋次数
     * @param customerpoolKey 奖池redis key
     * @param isSmallLevel
     * @param cardType        那种类型的许愿神灯
     * @return 获取砸蛋礼物Map
     */
    private Map<Integer, Temp> getTheCardGift(Integer giftNum, String customerpoolKey, String publicPoolKey,
                                              boolean isSmallLevel, Integer cardType) {
        List<Node> nodes = new ArrayList<>();

        //需要从新生成的奖池里拿出的礼物数量
        int genNum = 0;

        // 去个人奖池抽奖  如果当前个人奖池的数量小于用户需要翻牌的数量，就重新生成Pool
        if (isSmallLevel) {
            long size = redisHelper.sSize(customerpoolKey);
            logger.info("个人奖池中目前拥有的许愿神灯数量" + size);
            if (giftNum > size) {
                genNum = (int) (giftNum - size);
            }
        } else {
            long size = redisHelper.sSize(publicPoolKey);
            logger.info("公共奖池中目前拥有的许愿神灯数量" + size);
            if (giftNum > size) {
                genNum = (int) (giftNum - size);
            }
        }

        // 得到需要从旧奖池里取出来的礼物数量
        int oldNum = giftNum - genNum;
        logger.info("用户需要抽取的礼物数量" + giftNum);
        logger.info("需要从当前奖池中拿出来的礼物数量" + oldNum);
        logger.info("需要从新奖池中拿出来的礼物数量" + genNum);


        // 获取随机砸蛋奖品
        if (isSmallLevel) {
            if (genNum > 0) {
                // 获取当前配置的奖池需要生成的次数
                long configPoolSize = getConfigPoolSize(cardType, false);
                if (configPoolSize == 0) {
                    if (cardType == 1)
                        ExceptionUtils.throwResponseException(MarkCode.build("当前许愿神灯类的个人奖池尚未配置,请等管理员配置后再使用"));
                    else ExceptionUtils.throwResponseException(MarkCode.build("当前魔法神灯类的个人奖池尚未配置,请等管理员配置后再使用"));
                }
                logger.info("当前个人奖池配置每次可生成的许愿神灯数量" + configPoolSize);

                if (oldNum > 0) {
                    logger.info("从当前奖池中取礼物的数量" + oldNum);
                    nodes.addAll(redisHelper.sPop(customerpoolKey, oldNum, Node.class));
                }

                if (genNum < configPoolSize) {
                    logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
                    toGenCardPoolAndGetGift(customerpoolKey, genNum, (List<Node>) nodes, cardType, false);

                } else {
                    long num = genNum / configPoolSize;
                    logger.info("需要重复生成奖池的次数" + num);
                    long value = genNum % configPoolSize;
                    logger.info("求余" + value);
                    logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
                    if (value == 0) {
                        for (int i = 0; i < num; i++) {
                            toGenCardPoolAndGetGift(customerpoolKey, ((int) configPoolSize), (List<Node>) nodes, cardType, false);
                        }
                    } else {
                        logger.info("需要重复生成奖池的次数" + (num + 1));
                        for (int i = 0; i < num + 1; i++) {
                            if (i == num) {
                                toGenCardPoolAndGetGift(customerpoolKey, ((int) value), (List<Node>) nodes, cardType, false);
                            } else {
                                toGenCardPoolAndGetGift(customerpoolKey, ((int) configPoolSize), (List<Node>) nodes, cardType, false);
                            }

                        }
                    }

                }
            } else {
                logger.info("当前个人奖池的许愿神灯数量充足，无需重新生成");
                nodes.addAll(redisHelper.sPop(customerpoolKey, oldNum, Node.class));

            }
        } else {
            if (genNum > 0) {
                long configPoolSize = getConfigPoolSize(cardType, true);
                if (configPoolSize == 0) {
                    if (cardType == 1)
                        ExceptionUtils.throwResponseException(MarkCode.build("当前许愿神灯类的公共奖池尚未配置,请等管理员配置后再使用"));
                    else ExceptionUtils.throwResponseException(MarkCode.build("当前魔法神灯类的公共奖池尚未配置,请等管理员配置后再使用"));
                }
                logger.info("当前公共奖池配置每次可生成的许愿神灯数量" + configPoolSize);

                if (oldNum > 0) {
                    logger.info("从当前奖池中取礼物的数量" + oldNum);
                    nodes.addAll(redisHelper.sPop(publicPoolKey, oldNum, Node.class));
                }

                if (genNum < configPoolSize) {
                    logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
                    toGenCardPoolAndGetGift(publicPoolKey, genNum, (List<Node>) nodes, cardType, true);
                } else {
                    long num = genNum / configPoolSize;
                    logger.info("需要重复生成奖池的次数" + num);
                    long value = genNum % configPoolSize;
                    logger.info("求余" + value);
                    logger.info("奖池只需要生成一次即可，要抽取的许愿神灯次数为" + genNum);
                    if (value == 0) {
                        for (int i = 0; i < num; i++) {
                            toGenCardPoolAndGetGift(publicPoolKey, ((int) configPoolSize), (List<Node>) nodes, cardType, true);
                        }
                    } else {
                        logger.info("需要重复生成奖池的次数" + num + 1);
                        for (int i = 0; i < num + 1; i++) {
                            if (i == num) {
                                toGenCardPoolAndGetGift(publicPoolKey, ((int) value), (List<Node>) nodes, cardType, true);
                            } else {
                                toGenCardPoolAndGetGift(publicPoolKey, ((int) configPoolSize), (List<Node>) nodes, cardType, true);
                            }

                        }
                    }

                }
            } else {
                logger.info("当前公共奖池的许愿神灯数量充足，无需重新生成");
                nodes.addAll(redisHelper.sPop(publicPoolKey, oldNum, Node.class));
            }

        }

        logger.info("砸蛋结束：" + "nodes" + nodes.size() + "giftNum" + giftNum);
        Assert.isTrue(MarkCode.build("本期奖池礼物少于您所砸次数，请稍后"), nodes.size() == giftNum);

        Map<Integer, Temp> map = new HashMap<>(16);
        for (Node node : nodes) {
            Temp temp = map.get(node.getGiftId());
            if (Objects.isNull(temp)) {
                temp = new Temp(1, node.getSend());
                map.put(node.getGiftId(), temp);
            } else {
                temp.setNum(temp.getNum() + 1);
            }
        }
        return map;
    }


//    /**
//     * 查看本期奖池
//     *
//     * @return 本期奖池
//     */
//    public List<GiftRes> listByCustomer(int type) {
//        Customer customer = customerService.findById(ShiroUtils.getCustomerId());
//        AssetInfo assetInfo = customer.getAssetInfo();
//        int peerage = assetInfo.getPeerage();
//        boolean isSmallLevel = peerage < 2 ? true : false;
//        logger.info("查看本期奖池" + isSmallLevel + "type" + type);
//        List<GiftRes> result = new ArrayList<>();
//        List<TheCard> theCards;
//        if (isSmallLevel) {
//            theCards = this.listPollByCondition(false, type);
//        } else {
//            theCards = this.listPollByCondition(true, type);
//        }
//        if (CollectionUtils.isNotEmpty(theCards)) {
//            theCards.forEach(theCard -> result.add(giftMapper.toRes(theCard.getGift())));
//        }
//        logger.info("查看本期奖池end");
//        return result;
//    }

    /**
     * 查看本期奖池  取消掉了个人奖池
     *
     * @return 本期奖池
     */
    public List<GiftRes> listByCustomer(int type) {
//        Customer customer = customerService.findById(ShiroUtils.getCustomerId());
//        AssetInfo assetInfo = customer.getAssetInfo();

        List<GiftRes> result = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();
        ids.add(0);
        ids.add(1);
        ids.add(2);
        ids.add(3);
        List<TheCard> theCards = this.listPollByCondition(ids, type);

        // 把虚拟礼物添加到本期奖池返回池中国
//        List<GiftCompensation> giftCompensations = giftCompensationService.listGiftAppoin();
//        if (giftCompensations != null) {
//            for (GiftCompensation compensation : giftCompensations) {
//                TheCard egg = new TheCard();
//                egg.setGift(compensation.getGift());
////                egg.setId(compensation.getId());
////                egg.setNumber(1);
////                egg.setOtherPool(true);
////                egg.setSend(true);
//                if (theCards == null) {
//                    theCards = new ArrayList<>();
//                }
//                theCards.add(0, egg);
//            }
//        } else {
//
//        }
        Set<Integer> giftIdSet = new HashSet<>();
        if (CollectionUtils.isNotEmpty(theCards)) {
            for (int i = 0; i < theCards.size(); i++) {
                if (!giftIdSet.contains(theCards.get(i).getGift().getId())) {
                    result.add(giftMapper.toRes(theCards.get(i).getGift()));
                    giftIdSet.add(theCards.get(i).getGift().getId());
                }
            }
        }
        logger.info("查看本期奖池end");
        return result;
    }


    /**
     * 不同许愿神灯类型的奖池的礼物
     *
     * @param otherPool 是否是公共奖池
     */
    public List<TheCard> listPollByCondition(List<Integer> otherPool, int type) {
        Specification<TheCard> spec = (Specification<TheCard>) (root, query, builder) -> {
            //进行精准的匹配  （比较的属性，比较的属性的取值）
            CriteriaBuilder.In<Integer> inIds = builder.in(root.get("otherPool"));
            otherPool.forEach(inIds::value);

            Predicate p2 = builder.equal(root.get("cardType"), type);
            return builder.and(inIds, p2);
        };


        return super.list(spec, Sort.by(Sort.Direction.DESC, "gift.coin"));
    }

    /**
     * 个人奖池2或者其他奖池 可直接抽奖的数据
     *
     * @param otherPool
     * @param type
     * @return
     */
    public List<TheCard> listPollByConditionNew(Integer otherPool, int type) {
        Specification<TheCard> spec = (Specification<TheCard>) (root, query, builder) -> {
            //进行精准的匹配  （比较的属性，比较的属性的取值）
            Predicate p1 = builder.equal(root.get("otherPool"), otherPool);
            Predicate p2 = builder.equal(root.get("cardType"), type);
            Predicate p3 = builder.equal(root.get("threshold"), 0);
            return builder.and(p1, p2, p3);
        };


        return super.list(spec, Sort.by(Sort.Direction.DESC, "gift.coin"));
    }

    /**
     * 个人奖池2特殊的必抽数据 按照阈值升序排列
     *
     * @param otherPool
     * @param type
     * @return
     */
    public List<TheCard> listPollByConditionNewForThreshold(Integer otherPool, int type) {
        Specification<TheCard> spec = (Specification<TheCard>) (root, query, builder) -> {
            //进行精准的匹配  （比较的属性，比较的属性的取值）
            Predicate p1 = builder.equal(root.get("otherPool"), otherPool);
            Predicate p2 = builder.equal(root.get("cardType"), type);
            Predicate p3 = builder.gt(root.get("threshold"), 0);
            return builder.and(p1, p2, p3);
        };


        return super.list(spec, Sort.by(Sort.Direction.ASC, "threshold"));
    }

    /**
     * 不同奖池的所有礼物中最大的礼物价值
     */
    public BigDecimal maxCoin() {
//        List<TheCard> tapEggs = listByCondition(true);
//        if (CollectionUtils.isNotEmpty(tapEggs)) {
//            return tapEggs.get(0).getGift().getCoin();
//        }
        return null;
    }

    /**
     * 构建砸蛋礼物返回值
     *
     * @param customerId 用户id
     * @param gift       砸中的礼物
     * @param num        礼物数量
     * @return 砸蛋礼物
     */
    public GiftTheCardRes toRes(Integer customerId, Gift gift, Integer num, Integer cardTypeId) {
        GiftTheCardRes cardRes = new GiftTheCardRes();
        cardRes.setCustomerId(customerId);
        cardRes.setId(gift.getId());
        cardRes.setCardTypeId(cardTypeId);
        cardRes.setName(gift.getName());
        cardRes.setImage(gift.getImage().getUrl());
        cardRes.setBig(gift.isBig());
        cardRes.setPrice(gift.getCoin().toString());
        Image largeImage = gift.getLargeImage();
        if (Objects.nonNull(largeImage)) {
            cardRes.setLargeImage(largeImage.getUrl());
        }
        cardRes.setNum(num);
        return cardRes;
    }

    /**
     * 生成砸蛋奖池
     *
     * @param key       奖池存储key
     * @param otherPool 是否是其他奖池
     */
    private void generateTapEggPool(String key, boolean otherPool, Integer type) {
        // warning listPollByCondition(1, type);
        List<TheCard> theCards = new ArrayList<>();
        for (TheCard card : theCards) {
            Integer giftId = card.getGift().getId();
            Boolean send = card.isSend();
            int number = (int) card.getNumber();
            if (number > 0) {
                Node[] nodes = new Node[number];
                for (int i = 0; i < number; i++) {
                    nodes[i] = new Node(giftId, send);
                }
                redisHelper.sSet(key, nodes);
            }
        }
    }

    private void generateTapEggPoolNew(String key, Integer otherPool, Integer type) {
        List<TheCard> theCards = listPollByConditionNew(otherPool, type);
        for (TheCard card : theCards) {
            Integer giftId = card.getGift().getId();
            Boolean send = card.isSend();
            int number = (int) card.getNumber();
            if (number > 0) {
                Node[] nodes = new Node[number];
                for (int i = 0; i < number; i++) {
                    nodes[i] = new Node(giftId, send, card.getThreshold(), card.getGift().getCoin());
                }
                redisHelper.sSet(key, nodes);
            }
        }
    }

    /**
     * 排序后 List形式存储
     *
     * @param key
     * @param otherPool
     * @param type
     */
    private void generateTapEggPoolNewForThreshold(String key, Integer otherPool, Integer type) {
        List<TheCard> theCards = listPollByConditionNewForThreshold(otherPool, type);
        List<Node> nodeList = new ArrayList<>();
        for (TheCard card : theCards) {
            Integer giftId = card.getGift().getId();
            Boolean send = card.isSend();
            int number = (int) card.getNumber();
            if (number > 0) {
                for (int i = 0; i < number; i++) {
                    nodeList.add(new Node(giftId, send, card.getThreshold(), card.getGift().getCoin()));
                }
            }
        }
        redisHelper.set(key, nodeList);
    }

    /**
     * 生成奖池并弹出礼物
     *
     * @param poolKey
     * @param genNum
     * @param nodes
     */
    private void toGenCardPoolAndGetGift(String poolKey, int genNum, List<Node> nodes, Integer type, boolean publicBool) {
        generateTapEggPool(poolKey, publicBool, type);
        nodes.addAll(redisHelper.sPop(poolKey, genNum, Node.class));
    }

    /**
     * 得到对应许愿神灯类型和对应的奖池最大生成数量
     *
     * @param cardType
     * @param isPublicPool
     * @return
     */
    private long getConfigPoolSize(Integer cardType, boolean isPublicPool) {
        Specification<TheCard> spec = (Specification<TheCard>) (root, query, builder) -> {
            //进行精准的匹配  （比较的属性，比较的属性的取值）
            Predicate p1 = builder.equal(root.get("otherPool"), isPublicPool);
            Predicate p2 = builder.equal(root.get("cardType"), cardType);
            return builder.and(p1, p2);
        };
        List<TheCard> list = list(spec);
        if (list == null || list.isEmpty()) {
            return 0;
        }
        long count = 0;
        for (TheCard card : list) {
            count += card.getNumber();
        }


        return count;
    }

    private long getConfigPoolSizeNew(Integer cardType, Integer isPublicPool) {
        Specification<TheCard> spec = (Specification<TheCard>) (root, query, builder) -> {
            //进行精准的匹配  （比较的属性，比较的属性的取值）
            Predicate p1 = builder.equal(root.get("otherPool"), isPublicPool);
            Predicate p2 = builder.equal(root.get("cardType"), cardType);
            return builder.and(p1, p2);
        };
        List<TheCard> list = list(spec);
        if (list == null || list.isEmpty()) {
            return 0;
        }
        long count = 0;
        for (TheCard card : list) {
            count += card.getNumber();
        }


        return count;
    }

    /**
     * 构建砸蛋聊天室消息/全服消息
     *
     * @param roomId     房间id
     * @param customer   砸蛋人
     * @param giftImage  礼物图片
     * @param giftNum    礼物数量
     * @param cardTypeId 许愿神灯类型ID
     * @return 砸蛋聊天室消息
     */
    private Map<String, Object> buildMessage(Integer roomId, Customer customer,
                                             String giftImage, Integer giftNum, Integer cardTypeId, String price, String giftName) {
        Map<String, Object> content = new HashMap<>(6);
        content.put("customerId", customer.getId());
        content.put("head", customer.getCustomerInfo().getAvatar());
        content.put("nickname", customer.getCustomerInfo().getNickname());
        content.put("roomId", roomId);
        content.put("giftImage", giftImage);
        content.put("giftNum", giftNum);
        content.put("cardTypeId", cardTypeId);
        content.put("price", price);
        content.put("giftName", giftName);
        return content;
    }

    /**
     * 发送砸蛋全服消息 extra_egg_all 客户端显示砸许愿神灯横幅通知
     *
     * @param senderId 发送者
     * @param messages 全服消息
     */
    private void sendBroadcastMessage(Integer senderId, Integer roomId, List<Map<String, Object>> messages) {
        if (CollectionUtils.isNotEmpty(messages)) {
            List<Integer> roomIds = OnlineHelper.onlineRoomIds();
            roomIds.removeIf(id -> id.equals(roomId));
            messageHelper.sendToAllRoom(senderId, roomIds, JSON.toJSONString(messages), "extra_egg_all");

            // 自己房间发送不显示消息
//            messages.forEach(m -> m.put("show", false));
            messageHelper.sendToAllRoom(senderId, new ArrayList<>(Collections.singletonList(roomId)),
                    JSON.toJSONString(messages), "extra_egg_all");
        }
    }

    /**
     * 推送消息
     *
     * @param messages 推送消息
     */
    private void sendPushMessage(List<String> messages) {
        if (CollectionUtils.isNotEmpty(messages)) {
            messages.forEach(message -> {
                PushPayload payload = jPushHelper.buildPushObject_all_all_alert(message);
                jPushHelper.sendPush(payload);
            });
        }
    }

    @Data
    public static class Node {

        /**
         * id
         */
        private String id = UUIDTool.getUUID();

        /**
         * 礼物id
         */
        private Integer giftId;

        /**
         * 是否发送全服消息
         */
        private Boolean send;

        /**
         * 个人奖池2 中消费满xx金币 必出礼物
         */
        private Integer threshold;

        /**
         * 礼物价值 用于算实时的奖池爆率
         */
        private BigDecimal coin;

        private Integer coinInteger;

        public Node() {
        }

        public Node(Integer giftId, Boolean send) {
            this.giftId = giftId;
            this.send = send;
        }


        public Node(Integer giftId, Boolean send, Integer threshold, BigDecimal coin) {
            this.giftId = giftId;
            this.send = send;
            this.threshold = threshold;
            this.coin = coin;
            this.coinInteger = coin.intValue();
        }
    }

    @Data
    private class Temp {

        /**
         * 礼物数量
         */
        private int num;

        /**
         * 是否发送全服消息
         */
        private boolean send;

        public Temp() {
        }

        public Temp(int num, boolean send) {
            this.num = num;
            this.send = send;
        }
    }
}
