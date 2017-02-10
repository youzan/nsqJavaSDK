package com.youzan.nsq.client.entity.lookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.youzan.nsq.client.configs.DCCMigrationControlConfig;
import com.youzan.util.HostUtil;
import com.youzan.util.NotThreadSafe;
import com.youzan.util.SystemUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class for migration control config.
 * Created by lin on 16/12/6.
 */
public abstract class AbstractControlConfig {
    private static final Logger logger = LoggerFactory.getLogger(AbstractControlConfig.class);

    private volatile boolean invalid = false;
    private List<SoftReference<SeedLookupdAddress>> seedsRef = new ArrayList<>();
    private int previous = 0, current = 0;
    private Gradation gradation;

    public void setPrevious(int preIdx) {
        this.previous = preIdx;
    }

    public void setCurrent(int curIdx) {
        this.current = curIdx;
    }

    public void setGradation(final Gradation grad) {
        this.gradation = grad;
    }

    public List<SoftReference<SeedLookupdAddress>> getPreviousReferences() {
        if(invalid)
            return null;
        return this.seedsRef.subList(this.previous, this.current);
    }

    public List<SoftReference<SeedLookupdAddress>> getCurrentReferences() {
        if(invalid)
            return null;
        return this.seedsRef.subList(this.current, this.seedsRef.size());
    }

    public Gradation getGradation() {
        return this.gradation;
    }

    protected void addSeedReference(SeedLookupdAddress seed) {
        this.seedsRef.add(new SoftReference<>(seed));
        SeedLookupdAddress.addReference(seed);
    }

    public List<SoftReference<SeedLookupdAddress>> getSeeds() {
        return this.seedsRef;
    }

    /**
     * clean reference and resource allocated by current Control Config
     */
    void clean() {
        //set all index to 0
        this.invalid = true;
        //clean seed lookup address and underline lookup address, all are reference
        for (SoftReference<SeedLookupdAddress> seedRef : this.seedsRef) {
            SeedLookupdAddress aSeed = seedRef.get();
            if (null != aSeed)
                SeedLookupdAddress.removeReference(aSeed);
            //clear anyway
            seedRef.clear();
        }
    }

    public static AbstractControlConfig create(final String ctrlcnfStr) {
        ObjectMapper mapper = SystemUtil.getObjectMapper();
        DCCMigrationControlConfig dccCtrlCnf = null;
        try {
            JsonNode ctrlCnfNode = mapper.readTree(ctrlcnfStr);
            dccCtrlCnf = new DCCMigrationControlConfig();
            //previous seed lookup
            JsonNode previousNodes = ctrlCnfNode.get("previous");
            if (null != previousNodes) {
                for (JsonNode preNode : previousNodes) {
                    dccCtrlCnf.addSeedReference(SeedLookupdAddress.create(preNode.asText()));
                }
            }

            //current seed lookup
            JsonNode currentNodes = ctrlCnfNode.get("current");
            if (null != currentNodes) {
                //set current position
                dccCtrlCnf.setCurrent(previousNodes.size());
                for (JsonNode curNode : currentNodes) {
                    dccCtrlCnf.addSeedReference(SeedLookupdAddress.create(curNode.asText()));
                }
            }

            //get gradation
            JsonNode gradatioNode = ctrlCnfNode.get("gradation");
            String localhost = HostUtil.getHostname();
            //initialize with origin percentage 100%
            int factor = 100;
            Gradation aGradation;
            if (null != localhost) {
                JsonNode hostNode = gradatioNode.get(localhost);
                if (null != hostNode) {
                    factor = hostNode.get("percent").asInt();
                } else {
                    JsonNode wildFactor = gradatioNode.get("*");
                    if(null != wildFactor)
                        factor = wildFactor.get("percent").asInt();
                }
            }
            aGradation = new Gradation(factor);
            dccCtrlCnf.setGradation(aGradation);
        } catch (NullPointerException npe) {
            logger.error("Invalid control config format from config access agent. Control config: {}.", ctrlcnfStr, npe);
        } catch (IOException e) {
            logger.error("Fail to parse control config from passin config string. Control config: {}.", ctrlcnfStr, e);
        }

        if (null != dccCtrlCnf && dccCtrlCnf.isValid())
            return dccCtrlCnf;
        return null;
    }

    /**
     * function to check if seed lookup addresses and gradation is not empty
     * @return {@link Boolean#TRUE} if current control config is valid.
     */
    public boolean isValid() {
        return ((this.current == 0 && this.previous == 0 && this.seedsRef.size() > 0) || (this.current > 0 && this.seedsRef.size() > 0)) && null != this.getGradation();
    }

    @NotThreadSafe
    public static class Gradation {
        private Percentage percent;

        public Gradation(int weight) {
            this.percent = new Percentage(weight);
        }

        public Percentage getPercentage() {
            return this.percent;
        }
    }

    @NotThreadSafe
    public static class Percentage {
        final private int factor;

        public Percentage(int factor) {
            this.factor = factor;
        }

        public int getFactor() {
            return this.factor;
        }
    }
}


